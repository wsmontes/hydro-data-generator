"""
sanctions.py — Portal da Transparência: sanctions.

Vínculos:
- ~3% de todos os CPF/CNPJ recebem sanção
- UCs com fraude confirmada em inspection → 40% de chance de sanção
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator


TIPOS_CADASTRO_PJ = ["CEIS", "CNEP"]
TIPOS_CADASTRO_PF = ["CEAF", "CEIS"]

ORGAOS_SANCIONADORES = [
    ("CGU", "DF"),
    ("TCU", "DF"),
    ("ANEEL", "DF"),
    ("Ministério Público Federal", "DF"),
    ("Tribunal de Contas do Estado de São Paulo", "SP"),
    ("PROCON-SP", "SP"),
    ("Secretaria da Fazenda SP", "SP"),
    ("IBAMA", "DF"),
    ("Receita Federal", "DF"),
]

CATEGORIAS = [
    "Impedimento de licitar e contratar",
    "Suspensão de participação em licitação",
    "Declaração de inidoneidade",
    "Multa administrativa",
    "Proibição de receber incentivos fiscais",
    "Suspensão de atividades",
]

DESCRICOES = [
    "Irregularidade em contrato administrativo",
    "Fraude em processo licitatório",
    "Descumprimento de obrigação contratual",
    "Inadimplência tributária reiterada",
    "Infração ambiental",
    "Utilização indevida de recursos públicos",
    "Sonegação fiscal comprovada",
    "Exploração irregular de serviço público",
]


class SanctionsGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        ext = self.config.get("external", {})
        sanction_rate = ext.get("sanction_rate", 0.03)
        fraud_boost = ext.get("sanction_fraud_boost", 0.40)

        customer_df = self.ctx["customer"]
        fraud_set = self.ctx.get("_fraud_cpf_cnpj", set())

        fraud_entities = customer_df[customer_df["cpf_cnpj"].isin(fraud_set)]
        normal_entities = customer_df[~customer_df["cpf_cnpj"].isin(fraud_set)]

        n_fraud_sanctions = int(len(fraud_entities) * fraud_boost)
        n_normal_sanctions = int(len(normal_entities) * sanction_rate)

        fraud_sample = (
            fraud_entities.sample(
                n=min(n_fraud_sanctions, len(fraud_entities)),
                random_state=self.seed,
            )
            if len(fraud_entities) > 0
            else pd.DataFrame()
        )
        normal_sample = (
            normal_entities.sample(
                n=min(n_normal_sanctions, len(normal_entities)),
                random_state=self.seed,
            )
            if len(normal_entities) > 0
            else pd.DataFrame()
        )

        sanctioned = pd.concat([fraud_sample, normal_sample], ignore_index=True)

        rows = []
        for i, (_, entity) in enumerate(sanctioned.iterrows()):
            tipo_pessoa = entity["tipo"]
            is_pj = tipo_pessoa == "PJ"
            tipo_cadastro = random.choice(
                TIPOS_CADASTRO_PJ if is_pj else TIPOS_CADASTRO_PF
            )

            orgao, uf_orgao = random.choice(ORGAOS_SANCIONADORES)

            data_inicio = self.random_date(
                start=self.date_start - pd.DateOffset(years=3),
                end=self.date_end,
            )
            ativo = random.random() < 0.65
            data_fim = None
            if not ativo:
                data_fim = data_inicio + pd.Timedelta(
                    days=random.randint(90, 1095)
                )
                if data_fim > self.date_end:
                    data_fim = self.date_end

            rows.append({
                "sanction_id": f"SANC-{i + 1:06d}",
                "tipo_cadastro": tipo_cadastro,
                "cpf_cnpj": entity["cpf_cnpj"],
                "nome": entity["nome"],
                "tipo_pessoa": tipo_pessoa,
                "orgao_sancionador": orgao,
                "uf_orgao": uf_orgao,
                "categoria": random.choice(CATEGORIAS),
                "descricao": random.choice(DESCRICOES),
                "data_inicio": data_inicio.strftime("%Y-%m-%d"),
                "data_fim": data_fim.strftime("%Y-%m-%d") if data_fim else "",
                "ativo": ativo,
                "numero_processo": (
                    f"PAD-{data_inicio.year}-{random.randint(100000, 999999):06d}"
                ),
                "valor_multa": round(random.uniform(500, 500_000), 2),
            })

        df = pd.DataFrame(rows)
        self.save(df, "sanctions")
        self.ctx["sanctions"] = df
        return df
