"""
conftest.py — Fixtures de sessão para os testes de qualidade.

A fixture `dfs` roda o gerador UMA vez por sessão pytest (seed fixo,
config dedicada) e disponibiliza todos os DataFrames carregados como dict.

Uso nos testes:
    def test_algo(dfs):
        cu = dfs["consumer_unit"]
        assert len(cu) > 0
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

# Garante que o root do projeto está no sys.path (necessário quando pytest
# é invocado de dentro do diretório tests/)
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from generate_all import run  # noqa: E402 — precisa do path acima

DEFAULT_CONFIG = str(ROOT / "tests" / "config_test.yaml")

# Nomes de todas as tabelas esperadas
ALL_TABLES = [
    "economic_activity",
    "address",
    "customer",
    "electrician",
    "meter_reader",
    "transformer",
    "consumer_unit",
    "meter_reading",
    "transformer_reading",
    "reading_occurrence",
    "meter_image",
    "reading_agent",
    "work_order",
    "inspection",
    "declared_load",
    "external_property_data",
]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--config",
        default=DEFAULT_CONFIG,
        metavar="PATH",
        help="Config YAML a usar para geração e teste do dataset "
             f"(padrão: {DEFAULT_CONFIG})",
    )
    parser.addoption(
        "--skip-generate",
        action="store_true",
        default=False,
        help="Pula a geração de dados e testa os CSVs já existentes no "
             "output_dir do config informado.",
    )


def _resolve_output_dir(config_path: str) -> Path:
    """Lê o output_dir do config YAML e resolve o caminho absoluto.

    Paths relativos são resolvidos a partir do root do projeto (ROOT),
    pois os geradores executam com cwd=ROOT.
    """
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    out = cfg.get("output_dir", "output")
    p = Path(out)
    if not p.is_absolute():
        p = (ROOT / out).resolve()
    return p


@pytest.fixture(scope="session")
def dfs(request: pytest.FixtureRequest) -> dict[str, pd.DataFrame]:
    """
    Executa o gerador (opcional) e retorna todos os CSVs como
    um dicionário {nome_tabela: DataFrame}.

    Opções de linha de comando:
        --config PATH         usa o config informado (padrão: config_test.yaml)
        --skip-generate       não regenera, apenas lê os CSVs existentes
    """
    config_path: str = request.config.getoption("--config")
    skip_generate: bool = request.config.getoption("--skip-generate")

    if not skip_generate:
        original_cwd = os.getcwd()
        os.chdir(ROOT)
        try:
            run(config_path=config_path)
        finally:
            os.chdir(original_cwd)

    output_dir = _resolve_output_dir(config_path)

    result: dict[str, pd.DataFrame] = {}
    for table in ALL_TABLES:
        path = output_dir / f"{table}.csv"
        result[table] = pd.read_csv(path)

    return result


@pytest.fixture(scope="session")
def output_dir(request: pytest.FixtureRequest) -> Path:
    """Diretório de saída dos CSVs resolvido a partir do config ativo."""
    return _resolve_output_dir(request.config.getoption("--config"))


@pytest.fixture(scope="session")
def config_params(request: pytest.FixtureRequest) -> dict:
    """Dicionário com os parâmetros do config YAML ativo."""
    config_path = request.config.getoption("--config")
    with open(config_path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Fixtures individuais por tabela (aliases convenientes)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def consumer_unit(dfs):
    return dfs["consumer_unit"]

@pytest.fixture(scope="session")
def meter_reading(dfs):
    return dfs["meter_reading"]

@pytest.fixture(scope="session")
def transformer_reading(dfs):
    return dfs["transformer_reading"]

@pytest.fixture(scope="session")
def inspection(dfs):
    return dfs["inspection"]

@pytest.fixture(scope="session")
def customer(dfs):
    return dfs["customer"]

@pytest.fixture(scope="session")
def address(dfs):
    return dfs["address"]

@pytest.fixture(scope="session")
def transformer(dfs):
    return dfs["transformer"]

@pytest.fixture(scope="session")
def electrician(dfs):
    return dfs["electrician"]

@pytest.fixture(scope="session")
def meter_reader(dfs):
    return dfs["meter_reader"]

@pytest.fixture(scope="session")
def work_order(dfs):
    return dfs["work_order"]

@pytest.fixture(scope="session")
def reading_occurrence(dfs):
    return dfs["reading_occurrence"]

@pytest.fixture(scope="session")
def meter_image(dfs):
    return dfs["meter_image"]

@pytest.fixture(scope="session")
def reading_agent(dfs):
    return dfs["reading_agent"]

@pytest.fixture(scope="session")
def declared_load(dfs):
    return dfs["declared_load"]

@pytest.fixture(scope="session")
def external_property_data(dfs):
    return dfs["external_property_data"]

@pytest.fixture(scope="session")
def economic_activity(dfs):
    return dfs["economic_activity"]


# ---------------------------------------------------------------------------
# External data fixtures (generated by generate_external.py)
# ---------------------------------------------------------------------------

EXTERNAL_TABLES = [
    "geo_municipio", "cnae", "company", "company_cnae", "company_qsa",
    "sanctions", "fraud_transaction", "fraud_score", "fraud_flags",
    "legal_process", "legal_party", "legal_movement",
    "financial_debt", "osint_events",
]

# Colunas que devem ser lidas como string (IDs com leading zeros)
_EXT_DTYPE_OVERRIDES: dict[str, dict[str, type]] = {
    "company":           {"cnpj": str, "cep": str},
    "company_cnae":      {"cnpj": str, "cnae": str},
    "company_qsa":       {"cnpj": str, "cpf_socio": str},
    "sanctions":         {"cpf_cnpj": str},
    "fraud_transaction": {"cpf_cnpj": str, "cep": str},
    "legal_party":       {"cpf_cnpj": str},
    "legal_process":     {"numero_cnj": str},
    "financial_debt":    {"cpf_cnpj": str},
    "osint_events":      {"cpf_cnpj": str},
    "cnae":              {"cnae": str, "grupo": str, "divisao": str},
}


@pytest.fixture(scope="session")
def ext_dfs(dfs, request):
    """
    Gera dados externos (Receita Federal, Serasa, Jusbrasil, etc.)
    sobre os CSVs base já gerados e retorna dict {nome: DataFrame}.
    """
    config_path: str = request.config.getoption("--config")
    skip_generate: bool = request.config.getoption("--skip-generate")

    if not skip_generate:
        from generate_external import run as run_external
        original_cwd = os.getcwd()
        os.chdir(ROOT)
        try:
            run_external(config_path=config_path)
        finally:
            os.chdir(original_cwd)

    output_dir_path = _resolve_output_dir(config_path)

    result: dict[str, pd.DataFrame] = {}
    for table in EXTERNAL_TABLES:
        path = output_dir_path / f"{table}.csv"
        if path.exists():
            dtypes = _EXT_DTYPE_OVERRIDES.get(table)
            result[table] = pd.read_csv(path, dtype=dtypes)

    return result
