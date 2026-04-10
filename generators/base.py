"""
base.py — Utilitários e classe base compartilhada por todos os geradores.
"""

from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Carregamento de configuração
# ---------------------------------------------------------------------------

def load_config(path: str = "config.yaml") -> dict[str, Any]:
    """Carrega o arquivo YAML de configuração."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Classe base
# ---------------------------------------------------------------------------

class BaseGenerator:
    """
    Classe base para todos os geradores de tabelas.

    Parâmetros
    ----------
    config : dict
        Configuração carregada do config.yaml.
    context : dict
        Dados já gerados por outros geradores (compartilhado entre todos).
    """

    def __init__(self, config: dict[str, Any], context: dict[str, pd.DataFrame]):
        self.config = config
        self.ctx = context
        self.seed: int = config.get("seed", 42)
        self.rng = np.random.default_rng(self.seed)
        random.seed(self.seed)

        self.output_dir = Path(config.get("output_dir", "./output"))
        self.output_format: str = config.get("output_format", "csv")

        start = config["date_range"]["start"]
        end = config["date_range"]["end"]
        self.date_start = pd.Timestamp(start)
        self.date_end = pd.Timestamp(end)

    # ------------------------------------------------------------------
    # Helpers de datas
    # ------------------------------------------------------------------

    def random_date(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        size: int = 1,
    ) -> pd.DatetimeIndex | pd.Timestamp:
        start = start or self.date_start
        end = end or self.date_end
        delta_days = (end - start).days
        offsets = self.rng.integers(0, delta_days + 1, size=size)
        dates = pd.to_datetime([start + pd.Timedelta(days=int(d)) for d in offsets])
        return dates if size > 1 else dates[0]

    def monthly_dates(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> pd.DatetimeIndex:
        start = start or self.date_start
        end = end or self.date_end
        return pd.date_range(start=start, end=end, freq="MS")

    # ------------------------------------------------------------------
    # Helpers numéricos
    # ------------------------------------------------------------------

    def normal_sample(self, mean: float, std: float, size: int = 1, low: float = 0.0) -> np.ndarray:
        """Amostragem normal truncada em `low`."""
        samples = self.rng.normal(loc=mean, scale=std, size=size)
        return np.clip(samples, low, None)

    def choice(self, options: list, weights: list | None = None, size: int = 1):
        if size == 1:
            return random.choices(options, weights=weights, k=1)[0]
        return random.choices(options, weights=weights, k=size)

    # ------------------------------------------------------------------
    # Persistência
    # ------------------------------------------------------------------

    def save(self, df: pd.DataFrame, table_name: str) -> Path:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        fmt = self.output_format.lower()
        if fmt == "csv":
            path = self.output_dir / f"{table_name}.csv"
            df.to_csv(path, index=False)
        elif fmt in ("parquet", "pq"):
            path = self.output_dir / f"{table_name}.parquet"
            df.to_parquet(path, index=False)
        else:
            raise ValueError(f"Formato desconhecido: {fmt}")
        print(f"  ✓ {table_name}: {len(df):,} linhas → {path}")
        return path

    def generate(self) -> pd.DataFrame:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Helpers globais (usados em múltiplos geradores)
# ---------------------------------------------------------------------------

SEASONAL_FACTORS = {
    1: 1.10, 2: 1.08, 3: 1.02, 4: 0.97,
    5: 0.93, 6: 0.90, 7: 0.89, 8: 0.92,
    9: 0.96, 10: 1.00, 11: 1.05, 12: 1.12,
}

CONSUMER_PROFILES = {
    "residencial_baixo":   {"mean": 120,  "std": 25,  "weight": 0.30},  # M3: era 80 kWh (abaixo do real)
    "residencial_medio":   {"mean": 220,  "std": 50,  "weight": 0.30},
    "residencial_alto":    {"mean": 600,  "std": 120, "weight": 0.10},
    "comercial_pequeno":   {"mean": 450,  "std": 100, "weight": 0.15},
    "comercial_medio":     {"mean": 1800, "std": 400, "weight": 0.08},
    "industrial":          {"mean": 6000, "std": 1500,"weight": 0.04},
    "rural":               {"mean": 150,  "std": 40,  "weight": 0.03},
}


# ---------------------------------------------------------------------------
# Carregamento de contexto existente (geração incremental)
# ---------------------------------------------------------------------------

def load_existing_context(config: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """
    Carrega CSVs/parquets já gerados para o dicionário de contexto.
    Reconstrói informação de fraude a partir de inspection + consumer_unit.
    """
    output_dir = Path(config.get("output_dir", "./output"))
    fmt = config.get("output_format", "csv").lower()

    tables = [
        "economic_activity", "address", "customer", "electrician",
        "meter_reader", "transformer", "consumer_unit", "meter_reading",
        "transformer_reading", "reading_occurrence", "meter_image",
        "reading_agent", "work_order", "inspection", "declared_load",
        "external_property_data",
    ]

    context: dict[str, pd.DataFrame] = {}
    for table in tables:
        ext = "csv" if fmt == "csv" else "parquet"
        path = output_dir / f"{table}.{ext}"
        if path.exists():
            df = pd.read_csv(path) if fmt == "csv" else pd.read_parquet(path)
            context[table] = df
            print(f"  ✓ {table}: {len(df):,} linhas carregadas")

    # Reconstrói informação de fraude a partir de inspection + consumer_unit
    if "inspection" in context and "consumer_unit" in context:
        fraud_consumer_ids = set(
            context["inspection"]
            .loc[
                context["inspection"]["resultado"] == "irregularidade_confirmada",
                "consumer_id",
            ]
            .unique()
        )
        cu_df = context["consumer_unit"].copy()
        cu_df["is_fraud"] = cu_df["consumer_id"].isin(fraud_consumer_ids)
        context["consumer_unit_full"] = cu_df

        fraud_cpf_cnpj = set(
            cu_df.loc[cu_df["is_fraud"], "cpf_cnpj"].dropna().unique()
        )
        context["_fraud_cpf_cnpj"] = fraud_cpf_cnpj
        print(f"  ✓ Entidades fraudulentas identificadas: {len(fraud_cpf_cnpj):,}")

    return context


def strip_doc(cpf_cnpj: str) -> str:
    """Remove formatação de CPF/CNPJ, retornando apenas dígitos com zero-padding."""
    raw = "".join(c for c in str(cpf_cnpj) if c.isdigit())
    if len(raw) > 11:
        return raw.zfill(14)   # CNPJ
    return raw.zfill(11)       # CPF
