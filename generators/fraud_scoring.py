"""
fraud_scoring.py — Serasa / Fraud Score:
  fraud_transaction, fraud_score, fraud_flags.

Vínculos:
- 100% dos CPF/CNPJ recebem score
- Entidades fraudulentas → score 600–950 (ALTO)
- Entidades normais → maioria 50–300 (BAIXO), ~15% 300–550 (MEDIO)
- Flags gerados apenas para nível ALTO
"""

from __future__ import annotations

import hashlib
import random

import pandas as pd
from faker import Faker

from generators.base import BaseGenerator

fake_br = Faker("pt_BR")
fake_br.seed_instance(42)


MOTIVOS_BAIXO = [
    "Perfil regular de consumo",
    "Sem indícios de irregularidade",
    "Cadastro consistente",
    "Histórico limpo",
]

MOTIVOS_MEDIO = [
    "Variação de consumo acima da média",
    "Inconsistência cadastral leve",
    "Alteração frequente de dados cadastrais",
    "Consultas recentes em série",
]

MOTIVOS_ALTO = [
    "Queda abrupta de consumo detectada",
    "Múltiplas reclamações registradas",
    "Histórico de inadimplência elevado",
    "Padrão atípico de consumo",
    "Divergência entre consumo e perfil declarado",
]

FLAG_DESCRICOES = {
    "LARANJA": [
        "CPF associado a múltiplas empresas recém-abertas",
        "Possível utilização de CPF de laranja",
        "CPF vinculado a empresa de fachada",
    ],
    "DOCUMENTO_INVALIDO": [
        "Dígito verificador inconsistente",
        "Documento com formatação irregular",
        "CPF/CNPJ não encontrado na base da Receita Federal",
    ],
    "EMAIL_RISCO": [
        "Email de domínio temporário detectado",
        "Email descartável identificado",
        "Domínio de email associado a fraudes anteriores",
    ],
    "TELEFONE_SUSPEITO": [
        "Telefone pré-pago sem correspondência cadastral",
        "Número de telefone associado a múltiplos cadastros",
        "DDD incompatível com endereço informado",
    ],
    "IDENTIDADE_INCONSISTENTE": [
        "Nome divergente entre cadastros",
        "Dados cadastrais conflitantes entre fontes",
        "Endereço não corresponde ao CEP informado",
    ],
}

FLAG_TIPOS = list(FLAG_DESCRICOES.keys())


def _det_uuid(seed_str: str) -> str:
    """UUID determinístico a partir de seed string."""
    h = hashlib.md5(seed_str.encode()).hexdigest()  # noqa: S324
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


class FraudScoringGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        customer_df = self.ctx["customer"]
        cu_df = self.ctx.get("consumer_unit_full", self.ctx.get("consumer_unit"))
        address_df = self.ctx["address"]
        fraud_set = self.ctx.get("_fraud_cpf_cnpj", set())

        # Endereço por cpf_cnpj
        cu_addr = cu_df[["cpf_cnpj", "endereco_id"]].drop_duplicates("cpf_cnpj")
        addr_merge = cu_addr.merge(address_df, on="endereco_id", how="left")
        addr_map = addr_merge.set_index("cpf_cnpj").to_dict("index")

        tx_rows = []
        score_rows = []
        flag_rows = []

        for i, (_, cust) in enumerate(customer_df.iterrows()):
            cpf_cnpj = cust["cpf_cnpj"]
            is_fraud = cpf_cnpj in fraud_set
            is_pj = cust["tipo"] == "PJ"

            transaction_id = _det_uuid(f"ft-{self.seed}-{i}")

            addr = addr_map.get(cpf_cnpj, {})
            cidade = addr.get("cidade", "Campinas")
            uf = addr.get("estado", "SP")

            data_consulta = self.random_date()

            tx_rows.append({
                "transaction_id": transaction_id,
                "cpf_cnpj": cpf_cnpj,
                "tipo_documento": "CNPJ" if is_pj else "CPF",
                "data_consulta": data_consulta.strftime("%Y-%m-%d %H:%M:%S"),
                "email": fake_br.email(),
                "telefone": fake_br.phone_number(),
                "cep": fake_br.postcode().replace("-", ""),
                "cidade": cidade,
                "uf": uf,
            })

            # ---- Score ----
            if is_fraud:
                score = round(random.uniform(600, 950), 1)
            else:
                r = random.random()
                if r < 0.80:
                    score = round(random.uniform(50, 300), 1)
                elif r < 0.95:
                    score = round(random.uniform(300, 550), 1)
                else:
                    score = round(random.uniform(550, 750), 1)

            prob = round(min(score / 1000, 0.99), 4)

            if score < 300:
                nivel = "BAIXO"
                motivo = random.choice(MOTIVOS_BAIXO)
            elif score < 550:
                nivel = "MEDIO"
                motivo = random.choice(MOTIVOS_MEDIO)
            else:
                nivel = "ALTO"
                motivo = random.choice(MOTIVOS_ALTO)

            score_rows.append({
                "transaction_id": transaction_id,
                "score": score,
                "probabilidade_fraude": prob,
                "nivel_risco": nivel,
                "motivo": motivo,
                "data_score": data_consulta.strftime("%Y-%m-%d %H:%M:%S"),
            })

            # ---- Flags (apenas ALTO) ----
            if nivel == "ALTO":
                n_flags = random.randint(1, 3)
                picked = random.sample(FLAG_TIPOS, k=min(n_flags, len(FLAG_TIPOS)))
                for ft in picked:
                    flag_rows.append({
                        "transaction_id": transaction_id,
                        "flag_tipo": ft,
                        "descricao": random.choice(FLAG_DESCRICOES[ft]),
                    })

        tx_df = pd.DataFrame(tx_rows)
        score_df = pd.DataFrame(score_rows)
        flags_df = (
            pd.DataFrame(flag_rows)
            if flag_rows
            else pd.DataFrame(columns=["transaction_id", "flag_tipo", "descricao"])
        )

        self.save(tx_df, "fraud_transaction")
        self.save(score_df, "fraud_score")
        self.save(flags_df, "fraud_flags")

        self.ctx["fraud_transaction"] = tx_df
        self.ctx["fraud_score"] = score_df
        self.ctx["fraud_flags"] = flags_df

        return tx_df
