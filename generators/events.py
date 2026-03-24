"""
events.py — Geradores de: reading_occurrence, meter_image, reading_agent.
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator

TIPOS_OCORRENCIA = [
    ("imovel_fechado",    0.40),
    ("leitura_estimada",  0.30),   # M1: era "media" — nome correto conforme ANEEL
    ("suspeita_fraude",   0.10),
    ("medidor_danificado",0.10),
    ("relogio_parado",    0.05),
    ("acesso_negado",     0.05),
]

OCC_TIPOS  = [t[0] for t in TIPOS_OCORRENCIA]
OCC_WEIGHTS = [t[1] for t in TIPOS_OCORRENCIA]

OBSERVACOES = [
    "Sem observações adicionais.",
    "Consumidor não estava presente.",
    "Lacre verificado e íntegro.",
    "Lacre com sinais de violação.",
    "Medidor com leitura incompatível com histórico.",
    "Identificado bypass externo.",
    "Consumidor solicitou revisão.",
    "Medidor inacessível — grades fechadas.",
]


class ReadingOccurrenceGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        mr_df = self.ctx["meter_reading_full"]
        rate = self.config.get("reading_occurrence_rate", 0.10)
        cu_full = self.ctx["consumer_unit_full"]
        fraud_ids = set(cu_full.loc[cu_full["is_fraud"], "consumer_id"])

        rows = []
        occ_idx = 1
        for _, reading in mr_df.iterrows():
            cid = reading["consumer_id"]
            # Fraudes têm probabilidade maior de gerar ocorrência
            p = rate * 3.0 if cid in fraud_ids else rate
            if random.random() < p:
                tipo = random.choices(OCC_TIPOS, weights=OCC_WEIGHTS, k=1)[0]
                # Fraudes tendem a tipo mais "interesting"
                if cid in fraud_ids and random.random() < 0.4:
                    tipo = "suspeita_fraude"
                rows.append({
                    "occurrence_id": f"OCC-{occ_idx:07d}",
                    "reading_id": reading["reading_id"],
                    "tipo_ocorrencia": tipo,
                    "observacao": random.choice(OBSERVACOES),
                })
                occ_idx += 1

        df = pd.DataFrame(rows)
        self.save(df, "reading_occurrence")
        return df


class MeterImageGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        mr_df = self.ctx["meter_reading_full"]
        rate = self.config.get("meter_image_rate", 0.20)

        rows = []
        img_idx = 1
        for _, reading in mr_df.iterrows():
            if random.random() < rate:
                ts = pd.to_datetime(reading["data_leitura"]) + pd.Timedelta(
                    hours=int(random.randint(7, 17)),
                    minutes=int(random.randint(0, 59)),
                )
                rows.append({
                    "image_id": f"IMG-{img_idx:07d}",
                    "reading_id": reading["reading_id"],
                    "path_arquivo": f"/images/{reading['consumer_id']}/{reading['reading_id']}.jpg",
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                })
                img_idx += 1

        df = pd.DataFrame(rows)
        self.save(df, "meter_image")
        return df


class ReadingAgentGenerator(BaseGenerator):
    """Tabela de ligação entre leitura e leiturista."""

    def generate(self) -> pd.DataFrame:
        mr_df = self.ctx["meter_reading_full"]

        df = mr_df[["reading_id", "_reader_id"]].copy()
        df = df.rename(columns={"_reader_id": "reader_id"})

        self.save(df, "reading_agent")
        return df
