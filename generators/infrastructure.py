"""
infrastructure.py — Geradores de: transformer.
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator

TIPOS_TRANSFORMADOR = ["aéreo", "subterrâneo", "pedestal"]
CAPACIDADES_KVA = [15, 30, 45, 75, 112.5, 150, 225, 300, 500]


class TransformerGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        n = self.config["volumes"]["transformers"]
        address_ids = list(self.ctx["address"]["endereco_id"])

        # E4: distribui transformadores uniformemente — máx 3 por endereço
        # Cria slots ciclando pelos endereços, embaralha e corta em n
        import math
        max_per_addr = max(2, math.ceil(n / len(address_ids)))
        slots: list[str] = []
        for _ in range(max_per_addr):
            shuffled = list(address_ids)
            random.shuffle(shuffled)
            slots.extend(shuffled)
        random.shuffle(slots)
        localizacoes = slots[:n]

        rows = []
        for i in range(1, n + 1):
            rows.append({
                "transformer_id": f"TR-{i:04d}",
                "localizacao": localizacoes[i - 1],
                "capacidade_kva": random.choice(CAPACIDADES_KVA),
                "tipo": random.choice(TIPOS_TRANSFORMADOR),
            })
        df = pd.DataFrame(rows)
        self.save(df, "transformer")
        return df
