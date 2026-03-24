"""
test_generators.py — Testa se os scripts de geração executam corretamente
e produzem os arquivos esperados com volumes coerentes.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent

ALL_TABLES = [
    "economic_activity", "address", "customer", "electrician", "meter_reader",
    "transformer", "consumer_unit", "meter_reading", "transformer_reading",
    "reading_occurrence", "meter_image", "reading_agent", "work_order",
    "inspection", "declared_load", "external_property_data",
]


class TestArquivosGerados:
    """Todos os arquivos CSV esperados foram criados."""

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_arquivo_existe(self, table, dfs, output_dir):
        # dfs dispara a geração; aqui só verifica o arquivo em disco
        path = output_dir / f"{table}.csv"
        assert path.exists(), f"Arquivo não encontrado: {path}"

    @pytest.mark.parametrize("table", ALL_TABLES)
    def test_arquivo_nao_vazio(self, table, dfs, output_dir):
        path = output_dir / f"{table}.csv"
        assert path.stat().st_size > 0, f"Arquivo vazio: {table}.csv"


class TestVolumes:
    """Volumes coerentes com o config ativo (qualquer config)."""

    def _cfg(self, request) -> dict:
        import yaml
        with open(request.config.getoption("--config")) as f:
            return yaml.safe_load(f)

    def _months(self, cfg) -> int:
        from datetime import date
        s = date.fromisoformat(cfg["date_range"]["start"])
        e = date.fromisoformat(cfg["date_range"]["end"])
        return (e.year - s.year) * 12 + (e.month - s.month) + 1

    def test_consumer_unit_volume(self, consumer_unit, request):
        n = self._cfg(request)["volumes"]["consumer_units"]
        assert len(consumer_unit) == n

    def test_meter_reading_volume(self, meter_reading, request):
        cfg = self._cfg(request)
        n_uc = cfg["volumes"]["consumer_units"]
        m    = self._months(cfg)
        expected = n_uc * m
        # Tolera ±5% (UCs suspensas/cortadas geram menos leituras)
        assert expected * 0.95 <= len(meter_reading) <= expected * 1.05, (
            f"meter_reading={len(meter_reading)}, esperado ~{expected}"
        )

    def test_transformer_reading_volume(self, transformer_reading, request):
        cfg = self._cfg(request)
        n_tr = cfg["volumes"]["transformers"]
        m    = self._months(cfg)
        expected = n_tr * m
        assert expected * 0.95 <= len(transformer_reading) <= expected * 1.05

    def test_customer_volume(self, customer, request):
        n = self._cfg(request)["volumes"]["consumer_units"]
        # Q2: customer.csv é filtrado para os CPF/CNPJs efetivamente usados em UCs;
        # com clientes multi-UC (~5% com 2-3 UCs), o total fica entre 55% e 100% de n.
        assert len(customer) >= int(n * 0.55), (
            f"customer.csv esperava ao menos {int(n*0.55)} linhas, encontrou {len(customer)}"
        )

    def test_inspection_volume(self, inspection, request):
        n = self._cfg(request)["volumes"]["inspections"]
        assert len(inspection) == n

    def test_economic_activity_volume(self, economic_activity):
        # Fixo: 20 comercial + 12 industrial + 11 rural
        assert len(economic_activity) == 43

    def test_address_volume(self, address):
        # Fixo: 20 endereços reais de SP
        assert len(address) == 20


class TestDeterminismo:
    """
    Mesma config + mesmo seed → mesma quantidade de linhas e
    mesmo consumer_id na primeira linha.
    """

    def test_reprodutibilidade_consumer_unit(self, dfs, request, output_dir):
        from generate_all import run

        config_path = request.config.getoption("--config")
        original_cwd = os.getcwd()
        os.chdir(ROOT)
        try:
            run(config_path=config_path)
        finally:
            os.chdir(original_cwd)

        cu2 = pd.read_csv(output_dir / "consumer_unit.csv")
        assert list(dfs["consumer_unit"]["consumer_id"]) == list(cu2["consumer_id"]), (
            "A geração não é determinística: consumer_ids diferem entre duas execuções"
        )

    def test_reprodutibilidade_meter_reading_len(self, dfs, output_dir):
        mr2 = pd.read_csv(output_dir / "meter_reading.csv")
        assert len(dfs["meter_reading"]) == len(mr2)
