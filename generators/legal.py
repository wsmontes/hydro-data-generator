"""
legal.py — Jusbrasil / Processos: legal_process, legal_party, legal_movement.

Vínculos:
- ~8% dos CPF/CNPJ envolvidos em processos judiciais
- Fraudulentos têm taxa 3× maior
- Distribuidora de energia aparece como AUTOR em cobranças cíveis
- Ministério Público aparece como AUTOR em processos criminais
"""

from __future__ import annotations

import random

import pandas as pd
from faker import Faker

from generators.base import BaseGenerator

fake_br = Faker("pt_BR")
fake_br.seed_instance(42)

EMPRESA_ENERGIA = "Distribuidora Hidro Energia S.A."
EMPRESA_CNPJ = "12.345.678/0001-99"

# tribunal → (ramo_justiça, código_TR) conforme padrão CNJ
TRIBUNAIS_SP = {
    "TJSP":  ("8", "26"),   # Justiça Estadual, São Paulo
    "TRT15": ("5", "15"),   # Justiça do Trabalho, 15ª Região
    "TRF3":  ("4", "03"),   # Justiça Federal, 3ª Região
}

AREAS_ASSUNTOS = {
    "CIVEL": [
        "Cobrança de energia elétrica",
        "Revisão de tarifas de energia",
        "Danos morais por corte indevido",
        "Restituição de valores cobrados indevidamente",
        "Obrigação de fazer - religação",
    ],
    "CRIMINAL": [
        "Furto de energia elétrica (Art. 155 CP)",
        "Adulteração de medidor (Art. 171 CP)",
        "Estelionato (Art. 171 CP)",
    ],
    "TRABALHISTA": [
        "Reclamatória trabalhista",
        "Acidente de trabalho",
        "Horas extras e adicional de periculosidade",
    ],
    "ADMINISTRATIVO": [
        "Recurso administrativo ANEEL",
        "Multa regulatória",
        "Contestação de penalidade",
    ],
}

TIPOS_MOVIMENTO = ["DECISAO", "PETICAO", "AUDIENCIA", "SENTENCA"]

DESCRICOES_MOVIMENTO = {
    "DECISAO": [
        "Decisão interlocutória — citação do réu",
        "Decisão — deferimento de tutela antecipada",
        "Decisão — indeferimento de prova pericial",
        "Decisão — designação de audiência de conciliação",
    ],
    "PETICAO": [
        "Petição inicial distribuída",
        "Contestação apresentada pelo réu",
        "Réplica à contestação",
        "Petição de juntada de documentos",
    ],
    "AUDIENCIA": [
        "Audiência de conciliação realizada",
        "Audiência de instrução e julgamento",
        "Audiência redesignada a pedido das partes",
    ],
    "SENTENCA": [
        "Sentença — procedente",
        "Sentença — improcedente",
        "Sentença — parcialmente procedente",
        "Sentença homologatória de acordo",
    ],
}


def _gen_cnj(year: int, seq: int, j: str = "8", tr: str = "26") -> str:
    """Gera número CNJ: NNNNNNN-DD.AAAA.J.TR.OOOO (20 dígitos)."""
    nnnnnnn = f"{seq:07d}"
    dd = f"{random.randint(0, 99):02d}"
    aaaa = str(year)
    oooo = f"{random.randint(1, 500):04d}"
    return f"{nnnnnnn}-{dd}.{aaaa}.{j}.{tr}.{oooo}"


class LegalGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        ext = self.config.get("external", {})
        process_rate = ext.get("legal_process_rate", 0.08)
        fraud_boost = ext.get("legal_fraud_boost_factor", 3)

        customer_df = self.ctx["customer"]
        fraud_set = self.ctx.get("_fraud_cpf_cnpj", set())

        fraud_customers = customer_df[customer_df["cpf_cnpj"].isin(fraud_set)]
        normal_customers = customer_df[~customer_df["cpf_cnpj"].isin(fraud_set)]

        n_fraud = int(len(fraud_customers) * min(process_rate * fraud_boost, 1.0))
        n_normal = int(len(normal_customers) * process_rate)

        fraud_sample = (
            fraud_customers.sample(
                n=min(n_fraud, len(fraud_customers)), random_state=self.seed
            )
            if len(fraud_customers) > 0
            else pd.DataFrame()
        )
        normal_sample = (
            normal_customers.sample(
                n=min(n_normal, len(normal_customers)), random_state=self.seed
            )
            if len(normal_customers) > 0
            else pd.DataFrame()
        )

        involved = pd.concat([fraud_sample, normal_sample], ignore_index=True)

        proc_rows = []
        party_rows = []
        move_rows = []
        proc_idx = 1

        for _, entity in involved.iterrows():
            cpf_cnpj = entity["cpf_cnpj"]
            nome = entity["nome"]
            is_fraud = cpf_cnpj in fraud_set

            # Área do processo — fraudulentos mais propensos a criminal
            if is_fraud:
                area = random.choices(
                    ["CIVEL", "CRIMINAL", "TRABALHISTA", "ADMINISTRATIVO"],
                    weights=[30, 45, 5, 20],
                    k=1,
                )[0]
            else:
                area = random.choices(
                    ["CIVEL", "CRIMINAL", "TRABALHISTA", "ADMINISTRATIVO"],
                    weights=[60, 5, 20, 15],
                    k=1,
                )[0]

            assunto = random.choice(AREAS_ASSUNTOS[area])
            tribunal = random.choice(list(TRIBUNAIS_SP.keys()))
            j_code, tr_code = TRIBUNAIS_SP[tribunal]

            data_dist = self.random_date(
                start=self.date_start - pd.DateOffset(years=2),
                end=self.date_end,
            )
            status = random.choices(
                ["ATIVO", "ENCERRADO"], weights=[60, 40], k=1
            )[0]

            data_ult = data_dist + pd.Timedelta(days=random.randint(30, 730))
            if data_ult > self.date_end:
                data_ult = self.date_end

            valor = round(random.uniform(1_000, 200_000), 2)
            if area == "CRIMINAL":
                valor = round(random.uniform(500, 50_000), 2)

            processo_id = f"PROC-{proc_idx:06d}"
            cnj = _gen_cnj(data_dist.year, proc_idx, j=j_code, tr=tr_code)

            proc_rows.append({
                "processo_id": processo_id,
                "numero_cnj": cnj,
                "tribunal": tribunal,
                "uf": "SP",
                "instancia": random.choice([1, 1, 1, 2]),
                "area": area,
                "assunto": assunto,
                "valor_causa": valor,
                "status": status,
                "data_distribuicao": data_dist.strftime("%Y-%m-%d"),
                "data_ultima_movimentacao": data_ult.strftime("%Y-%m-%d"),
            })

            # ---- Partes ----
            if area in ("CIVEL", "ADMINISTRATIVO"):
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": EMPRESA_CNPJ,
                    "nome": EMPRESA_ENERGIA,
                    "papel": "AUTOR",
                })
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": cpf_cnpj,
                    "nome": nome,
                    "papel": "REU",
                })
            elif area == "CRIMINAL":
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": "",
                    "nome": "Ministério Público do Estado de São Paulo",
                    "papel": "AUTOR",
                })
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": cpf_cnpj,
                    "nome": nome,
                    "papel": "REU",
                })
            else:  # TRABALHISTA
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": cpf_cnpj,
                    "nome": nome,
                    "papel": "AUTOR",
                })
                party_rows.append({
                    "processo_id": processo_id,
                    "cpf_cnpj": EMPRESA_CNPJ,
                    "nome": EMPRESA_ENERGIA,
                    "papel": "REU",
                })

            # Advogado
            party_rows.append({
                "processo_id": processo_id,
                "cpf_cnpj": "",
                "nome": f"Adv. {fake_br.name()}",
                "papel": "ADVOGADO",
            })

            # ---- Movimentações (2–5 por processo) ----
            n_moves = random.randint(2, 5)
            move_date = data_dist
            for m in range(n_moves):
                tipo = random.choice(TIPOS_MOVIMENTO)
                if m == 0:
                    tipo = "PETICAO"
                elif m == n_moves - 1 and status == "ENCERRADO":
                    tipo = "SENTENCA"

                move_rows.append({
                    "processo_id": processo_id,
                    "data": move_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "tipo": tipo,
                    "descricao": random.choice(DESCRICOES_MOVIMENTO[tipo]),
                })
                move_date = move_date + pd.Timedelta(
                    days=random.randint(15, 180)
                )
                if move_date > self.date_end:
                    move_date = self.date_end

            proc_idx += 1

        proc_df = pd.DataFrame(proc_rows)
        party_df = pd.DataFrame(party_rows)
        move_df = pd.DataFrame(move_rows)

        self.save(proc_df, "legal_process")
        self.save(party_df, "legal_party")
        self.save(move_df, "legal_movement")

        self.ctx["legal_process"] = proc_df
        self.ctx["legal_party"] = party_df
        self.ctx["legal_movement"] = move_df

        return proc_df
