"""
readings.py — Geradores de: meter_reading e transformer_reading.

Padrões de fraude injetados em meter_reading:
  - gradual_drop: consumo cai progressivamente ao longo do tempo
  - reader_corruption: leituras abaixo da média concentradas num reader_id suspeito
"""

from __future__ import annotations

import random
import uuid

import numpy as np
import pandas as pd

from generators.base import BaseGenerator, CONSUMER_PROFILES, SEASONAL_FACTORS


class MeterReadingGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        cu_df = self.ctx["consumer_unit_full"]
        fraud_cfg = self.config["fraud"]
        reader_ids = list(self.ctx["meter_reader"]["reader_id"])

        # Leituristas suspeitos: 1 por cada 8 leituristas
        n_corrupt_readers = max(1, len(reader_ids) // 8)
        corrupt_readers = set(reader_ids[:n_corrupt_readers])

        # Subset de UCs com padrão reader_corruption
        fraud_uc_ids = cu_df.loc[cu_df["is_fraud"], "consumer_id"].tolist()
        n_reader_fraud = int(len(fraud_uc_ids) * fraud_cfg["reader_corruption_pct"])
        reader_fraud_ucs = set(random.sample(fraud_uc_ids, k=min(n_reader_fraud, len(fraud_uc_ids))))

        # Subset de UCs com queda gradual
        n_drop = int(len(fraud_uc_ids) * fraud_cfg["gradual_drop_pct"])
        drop_fraud_ucs = set(random.sample(fraud_uc_ids, k=min(n_drop, len(fraud_uc_ids))))

        months = pd.date_range(
            start=self.date_start,
            end=self.date_end,
            freq="MS",
        )
        n_months = len(months)

        rows = []
        for _, uc in cu_df.iterrows():
            cid = uc["consumer_id"]
            profile = CONSUMER_PROFILES[uc["perfil_consumo"]]
            base_mean = profile["mean"]
            base_std = profile["std"]

            is_gradual_drop = cid in drop_fraud_ucs
            is_reader_corrupt = cid in reader_fraud_ucs

            # MR-1: Grupo A é definido pelo tipo de ligação, não pelo setor.
            # Trifásico e AT têm medição de demanda; mono/bi não.
            is_grupo_a = uc["tipo_ligacao"] in {"trifasico", "AT"}
            # S1 (fix): demanda derivada do perfil de consumo com fator de carga realista
            #   AT: grandes consumidores → range fixo 150–500 kW
            #   Trifásico BT: peak_demand = (base_mean / 720) / load_factor
            #     comercial LF 25–45% → ~5–11 kW
            #     industrial LF 35–65% → ~13–24 kW
            if is_grupo_a:
                if uc["tipo_ligacao"] == "AT":
                    demanda_base = float(self.rng.uniform(150, 500))
                else:
                    avg_power = base_mean / 720.0  # kW médio
                    if uc["tipo_consumidor"] == "industrial":
                        lf = float(self.rng.uniform(0.35, 0.65))
                    else:  # comercial e outros
                        lf = float(self.rng.uniform(0.25, 0.45))
                    demanda_base = avg_power / lf
            else:
                demanda_base = None

            for m_idx, month_ts in enumerate(months):
                season = SEASONAL_FACTORS[month_ts.month]
                mean_kwh = base_mean * season

                # ------- injeção de padrões de fraude -------

                if is_gradual_drop:
                    # redução gradual: até -60% ao final da série
                    drop_factor = 1.0 - (0.60 * m_idx / max(n_months - 1, 1))
                    mean_kwh *= drop_factor

                if is_reader_corrupt:
                    # leiturista reduz 20–40% do valor
                    if random.random() < 0.7:
                        mean_kwh *= self.rng.uniform(0.6, 0.80)

                consumo = max(0.0, float(self.rng.normal(mean_kwh, base_std * 0.3)))
                consumo = round(consumo, 2)

                dias = int(self.rng.integers(28, 33))
                data_leitura = month_ts + pd.Timedelta(days=int(self.rng.integers(1, 5)))

                # R3: fatura emitida 5–15 dias após leitura (ciclo real de distribuidora)
                data_fat = data_leitura + pd.Timedelta(days=int(self.rng.integers(5, 16)))

                # normaliza para 30 dias
                consumo_norm = round(consumo * 30 / dias, 2) if dias > 0 else consumo

                # R1: demanda apenas para Grupo A (AT)
                if is_grupo_a:
                    # Q5: demanda varia ±10% em torno do valor base do UC
                    demanda = round(demanda_base * float(self.rng.uniform(0.90, 1.10)), 1)
                else:
                    demanda = None

                reader_id = (
                    random.choice(list(corrupt_readers))
                    if is_reader_corrupt and random.random() < 0.75
                    else random.choice(reader_ids)
                )

                rows.append({
                    "reading_id": f"RD-{len(rows) + 1:08d}",
                    "consumer_id": cid,
                    "data_leitura": data_leitura.strftime("%Y-%m-%d"),
                    "data_faturamento": data_fat.strftime("%Y-%m-%d"),
                    "consumo_kwh": consumo,
                    "demanda_kw": demanda,
                    "dias_entre_leituras": dias,
                    "consumo_normalizado_30d": consumo_norm,
                    # reader_id guardado internamente para reading_agent
                    "_reader_id": reader_id,
                })

        df = pd.DataFrame(rows)
        self.ctx["meter_reading_full"] = df  # com _reader_id para uso interno

        export_df = df.drop(columns=["_reader_id"])
        self.save(export_df, "meter_reading")
        return df


class TransformerReadingGenerator(BaseGenerator):
    """
    Gera leituras mensais por transformador.
    O consumo total dos consumidores é somado e um fator de perda é aplicado:
    - Transformadores normais: perda 3–8%
    - Transformadores com UCs fraudulentas: perda 12–30% (déficit artificial)
    """

    def generate(self) -> pd.DataFrame:
        mr_df = self.ctx["meter_reading_full"]
        cu_df = self.ctx["consumer_unit_full"]
        tr_df = self.ctx["transformer"]

        # Consumo mensal por transformador
        cu_tr = cu_df[["consumer_id", "transformador_id", "is_fraud"]].copy()
        mr_agg = (
            mr_df.assign(month=pd.to_datetime(mr_df["data_leitura"]).dt.to_period("M"))
            .groupby(["consumer_id", "month"])["consumo_kwh"]
            .sum()
            .reset_index()
        )
        merged = mr_agg.merge(cu_tr, on="consumer_id")

        # Conta UCs fraudulentas por transformador para graduar a perda (R2)
        fraud_count_by_tr = (
            cu_df[cu_df["is_fraud"]]
            .groupby("transformador_id")["consumer_id"]
            .count()
            .to_dict()
        )
        total_count_by_tr = (
            cu_df.groupby("transformador_id")["consumer_id"]
            .count()
            .to_dict()
        )

        rows = []
        for (tr_id, month), grp in merged.groupby(["transformador_id", "month"]):
            soma_consumidores = grp["consumo_kwh"].sum()

            # R2: faixa de perda proporcional à densidade de fraude no transformador
            n_fraud_uc  = fraud_count_by_tr.get(tr_id, 0)
            n_total_uc  = total_count_by_tr.get(tr_id, 1)
            fraud_density = n_fraud_uc / max(n_total_uc, 1)

            if fraud_density == 0:
                loss_factor = float(self.rng.uniform(1.03, 1.07))   # normal: 3–7%
            elif fraud_density < 0.20:
                loss_factor = float(self.rng.uniform(1.07, 1.12))   # leve: 7–12%
            else:
                # alta densidade: 10–15% (P2: teto em 15% — acima disso seria anomalia grave
                # visível até para o faturamento; perdas técnicas urbanas reais: 5-12%)
                loss_factor = float(self.rng.uniform(1.10, 1.15))

            energia_total = round(soma_consumidores * loss_factor, 2)
            data_ref = month.to_timestamp() + pd.Timedelta(days=int(self.rng.integers(0, 5)))

            rows.append({
                "reading_id": f"TRD-{len(rows) + 1:07d}",
                "transformer_id": tr_id,
                "data": data_ref.strftime("%Y-%m-%d"),
                "energia_total_kwh": energia_total,
                "soma_consumidores_kwh": round(soma_consumidores, 2),
                "perda_estimada_pct": round((loss_factor - 1) * 100, 2),
            })

        # Transformadores sem UCs atribuídas: gera leituras de carga nula
        # (perdas em vazio do núcleo — realista para equipamentos em espera)
        covered_tr = {r["transformer_id"] for r in rows}
        all_tr_ids = set(tr_df["transformer_id"].unique())
        stranded   = all_tr_ids - covered_tr
        if stranded:
            start = pd.Timestamp(self.config["date_range"]["start"])
            end   = pd.Timestamp(self.config["date_range"]["end"])
            months = pd.period_range(start=start, end=end, freq="M")
            for tr_id in sorted(stranded):
                kva = float(tr_df.loc[tr_df["transformer_id"] == tr_id, "capacidade_kva"].iloc[0])
                for month in months:
                    # perdas em vazio: energia do núcleo ≈ 3–7% da capacidade nominal (kWh/mês)
                    idle_loss_pct = float(self.rng.uniform(3.0, 7.0))
                    idle_kwh      = round(kva * 720 * idle_loss_pct / 100, 2)   # 720 h/mês
                    data_ref      = month.to_timestamp() + pd.Timedelta(days=int(self.rng.integers(0, 5)))
                    rows.append({
                        "reading_id":            f"TRD-{len(rows) + 1:07d}",
                        "transformer_id":        tr_id,
                        "data":                  data_ref.strftime("%Y-%m-%d"),
                        "energia_total_kwh":     idle_kwh,
                        "soma_consumidores_kwh": 0.0,
                        "perda_estimada_pct":    round(idle_loss_pct, 2),
                    })

        df = pd.DataFrame(rows)
        self.save(df, "transformer_reading")
        return df
