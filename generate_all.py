"""
generate_all.py — Orquestrador principal.

Uso:
    python generate_all.py                   # usa config.yaml padrão
    python generate_all.py --config config.yaml
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from generators.base import load_config
from generators.static import EconomicActivityGenerator, AddressGenerator
from generators.people import CustomerGenerator, ElectricianGenerator, MeterReaderGenerator
from generators.infrastructure import TransformerGenerator
from generators.consumer_unit import ConsumerUnitGenerator
from generators.readings import MeterReadingGenerator, TransformerReadingGenerator
from generators.events import ReadingOccurrenceGenerator, MeterImageGenerator, ReadingAgentGenerator
from generators.operations import WorkOrderGenerator, InspectionGenerator
from generators.extras import DeclaredLoadGenerator, ExternalPropertyDataGenerator


def run(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)
    context: dict = {}

    steps = [
        # (nome, classe, chave_no_contexto)
        ("economic_activity",    EconomicActivityGenerator,    "economic_activity"),
        ("address",              AddressGenerator,             "address"),
        ("customer",             CustomerGenerator,            "customer"),
        ("electrician",          ElectricianGenerator,         "electrician"),
        ("meter_reader",         MeterReaderGenerator,         "meter_reader"),
        ("transformer",          TransformerGenerator,         "transformer"),
        ("consumer_unit",        ConsumerUnitGenerator,        "consumer_unit_full"),
        ("meter_reading",        MeterReadingGenerator,        "meter_reading_full"),
        ("transformer_reading",  TransformerReadingGenerator,  "transformer_reading"),
        ("reading_occurrence",   ReadingOccurrenceGenerator,   "reading_occurrence"),
        ("meter_image",          MeterImageGenerator,          "meter_image"),
        ("reading_agent",        ReadingAgentGenerator,        "reading_agent"),
        ("work_order",           WorkOrderGenerator,           "work_order"),
        ("inspection",           InspectionGenerator,          "inspection"),
        ("declared_load",        DeclaredLoadGenerator,        "declared_load"),
        ("external_property_data", ExternalPropertyDataGenerator, "external_property_data"),
    ]

    print(f"\n{'='*60}")
    print(f"  Hydro Data Generator")
    print(f"  Config : {config_path}")
    print(f"  Formato: {config.get('output_format', 'csv').upper()}")
    print(f"  Volumes: {config['volumes']}")
    print(f"{'='*60}\n")

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
    print(f"  ✅ Concluído em {total_elapsed:.1f}s")
    print(f"  Arquivos gerados em: {Path(config.get('output_dir', './output')).resolve()}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hydro synthetic data generator")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Caminho para o arquivo de configuração YAML (default: config.yaml)",
    )
    args = parser.parse_args()
    run(args.config)
