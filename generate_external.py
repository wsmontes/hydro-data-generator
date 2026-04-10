"""
generate_external.py — Orquestrador de dados externos.

Carrega os CSVs base já gerados (output_dir) e gera tabelas adicionais:
  Receita Federal, Portal da Transparência, Serasa, Jusbrasil, IBGE,
  Financeiro e OSINT.

Uso:
    python generate_external.py                          # usa config.yaml
    python generate_external.py --config config_large.yaml
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from generators.base import load_config, load_existing_context
from generators.reference import GeoMunicipioGenerator, CnaeReferenceGenerator
from generators.company import CompanyGenerator
from generators.sanctions import SanctionsGenerator
from generators.fraud_scoring import FraudScoringGenerator
from generators.legal import LegalGenerator
from generators.financial import FinancialDebtGenerator
from generators.osint import OsintEventsGenerator


def run(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)

    print(f"\n{'='*60}")
    print(f"  Hydro Data Generator — Dados Externos")
    print(f"  Config : {config_path}")
    print(f"  Output : {config.get('output_dir', './output')}")
    print(f"{'='*60}\n")

    print("→ Carregando dados existentes...")
    context = load_existing_context(config)
    print()

    steps = [
        # (nome, classe, chave_no_contexto)
        ("geo_municipio",     GeoMunicipioGenerator,    "geo_municipio"),
        ("cnae (referência)", CnaeReferenceGenerator,   "cnae_ref"),
        ("company",           CompanyGenerator,          "company"),
        ("sanctions",         SanctionsGenerator,        "sanctions"),
        ("fraud_scoring",     FraudScoringGenerator,     "fraud_transaction"),
        ("legal",             LegalGenerator,            "legal_process"),
        ("financial_debt",    FinancialDebtGenerator,    "financial_debt"),
        ("osint_events",      OsintEventsGenerator,      "osint_events"),
    ]

    total_start = time.time()

    for step_name, GeneratorClass, ctx_key in steps:
        print(f"→ Gerando {step_name}...")
        t0 = time.time()
        generator = GeneratorClass(config=config, context=context)
        result = generator.generate()
        context[ctx_key] = result
        elapsed = time.time() - t0
        print(f"  └ {elapsed:.2f}s\n")

    total_elapsed = time.time() - total_start
    print(f"{'='*60}")
    print(f"  ✅ Dados externos concluídos em {total_elapsed:.1f}s")
    print(f"  Arquivos em: {Path(config.get('output_dir', './output')).resolve()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hydro external data generator")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Caminho para o arquivo de configuração YAML (default: config.yaml)",
    )
    args = parser.parse_args()
    run(args.config)
