"""
test_external.py — Testes de integridade e verossimilhança dos dados externos.
"""

from __future__ import annotations

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_fk(child, child_col, parent, parent_col):
    """Retorna registros do filho cujo FK não existe no pai."""
    valid = set(parent[parent_col].dropna().unique())
    mask = child[child_col].notna() & (child[child_col] != "") & ~child[child_col].isin(valid)
    return child[mask]


def strip_doc(val):
    return "".join(c for c in str(val) if c.isdigit())


# ---------------------------------------------------------------------------
# Tabelas de referência (IBGE)
# ---------------------------------------------------------------------------

class TestReference:

    def test_geo_municipio_non_empty(self, ext_dfs):
        assert len(ext_dfs["geo_municipio"]) > 0

    def test_geo_municipio_columns(self, ext_dfs):
        expected = {"municipio_id", "nome", "uf", "regiao", "mesorregiao", "microrregiao"}
        assert expected.issubset(set(ext_dfs["geo_municipio"].columns))

    def test_geo_municipio_covers_address_cities(self, ext_dfs, dfs):
        """Todas as cidades do address existem no geo_municipio."""
        cities = set(dfs["address"]["cidade"].unique())
        mun_names = set(ext_dfs["geo_municipio"]["nome"].unique())
        missing = cities - mun_names
        assert len(missing) == 0, f"Cidades sem registro em geo_municipio: {missing}"

    def test_cnae_non_empty(self, ext_dfs):
        assert len(ext_dfs["cnae"]) > 0

    def test_cnae_columns(self, ext_dfs):
        expected = {"cnae", "descricao", "grupo", "divisao", "secao"}
        assert expected.issubset(set(ext_dfs["cnae"].columns))


# ---------------------------------------------------------------------------
# Company (Receita Federal)
# ---------------------------------------------------------------------------

class TestCompany:

    def test_company_covers_all_pj(self, ext_dfs, dfs):
        """Todo PJ do customer deve ter registro em company."""
        customer = dfs["customer"]
        pj_cnpjs = set(
            customer[customer["tipo"] == "PJ"]["cpf_cnpj"].apply(strip_doc)
        )
        company_cnpjs = set(ext_dfs["company"]["cnpj"].astype(str))
        missing = pj_cnpjs - company_cnpjs
        assert len(missing) == 0, f"{len(missing)} PJ customers sem registro em company"

    def test_cnpj_14_digits(self, ext_dfs):
        cnpjs = ext_dfs["company"]["cnpj"].astype(str)
        invalid = cnpjs[cnpjs.str.len() != 14]
        assert len(invalid) == 0, f"{len(invalid)} CNPJs com tamanho ≠ 14"

    def test_company_cnae_fk(self, ext_dfs):
        orphans = check_fk(ext_dfs["company_cnae"], "cnpj", ext_dfs["company"], "cnpj")
        assert len(orphans) == 0, f"{len(orphans)} company_cnae com CNPJ órfão"

    def test_company_qsa_fk(self, ext_dfs):
        orphans = check_fk(ext_dfs["company_qsa"], "cnpj", ext_dfs["company"], "cnpj")
        assert len(orphans) == 0, f"{len(orphans)} company_qsa com CNPJ órfão"

    def test_qsa_cpf_11_digits(self, ext_dfs):
        cpfs = ext_dfs["company_qsa"]["cpf_socio"].astype(str)
        invalid = cpfs[cpfs.str.len() != 11]
        assert len(invalid) == 0, f"{len(invalid)} CPFs de sócio com tamanho ≠ 11"

    def test_qsa_participation_sums_reasonable(self, ext_dfs):
        """Participação por empresa deve somar ~100%."""
        sums = ext_dfs["company_qsa"].groupby("cnpj")["percentual_participacao"].sum()
        bad = sums[(sums < 99.0) | (sums > 101.0)]
        assert len(bad) == 0, f"{len(bad)} empresas com participação ≠ ~100%"

    def test_all_companies_have_primary_cnae(self, ext_dfs):
        primary = ext_dfs["company_cnae"][ext_dfs["company_cnae"]["tipo"] == "PRINCIPAL"]
        cnpjs_with_primary = set(primary["cnpj"])
        all_cnpjs = set(ext_dfs["company"]["cnpj"])
        missing = all_cnpjs - cnpjs_with_primary
        assert len(missing) == 0, f"{len(missing)} empresas sem CNAE principal"


# ---------------------------------------------------------------------------
# Sanctions (Portal da Transparência)
# ---------------------------------------------------------------------------

class TestSanctions:

    def test_sanctions_non_empty(self, ext_dfs):
        assert len(ext_dfs["sanctions"]) > 0

    def test_sanctions_columns(self, ext_dfs):
        expected = {
            "sanction_id", "tipo_cadastro", "cpf_cnpj", "nome",
            "tipo_pessoa", "orgao_sancionador", "uf_orgao", "categoria",
            "descricao", "data_inicio", "ativo",
        }
        assert expected.issubset(set(ext_dfs["sanctions"].columns))

    def test_sanctions_tipo_cadastro_valid(self, ext_dfs):
        valid = {"CEIS", "CNEP", "CEPIM", "CEAF"}
        actual = set(ext_dfs["sanctions"]["tipo_cadastro"].unique())
        assert actual.issubset(valid)

    def test_fraud_entities_more_sanctioned(self, ext_dfs, dfs):
        """Taxa de sanção em fraudulentos deve superar a taxa em normais."""
        inspection = dfs["inspection"]
        cu = dfs["consumer_unit"]
        fraud_cids = set(
            inspection.loc[
                inspection["resultado"] == "irregularidade_confirmada",
                "consumer_id",
            ]
        )
        fraud_cpfs = set(cu[cu["consumer_id"].isin(fraud_cids)]["cpf_cnpj"])

        sanctions = ext_dfs["sanctions"]
        sanctioned_cpfs = set(sanctions["cpf_cnpj"])

        all_cpfs = set(dfs["customer"]["cpf_cnpj"])
        normal_cpfs = all_cpfs - fraud_cpfs

        fraud_rate = len(sanctioned_cpfs & fraud_cpfs) / max(len(fraud_cpfs), 1)
        normal_rate = len(sanctioned_cpfs & normal_cpfs) / max(len(normal_cpfs), 1)

        assert fraud_rate > normal_rate, (
            f"Taxa de sanção em fraudulentos ({fraud_rate:.3f}) deveria superar "
            f"a taxa em normais ({normal_rate:.3f})"
        )


# ---------------------------------------------------------------------------
# Fraud Scoring (Serasa)
# ---------------------------------------------------------------------------

class TestFraudScoring:

    def test_100pct_coverage(self, ext_dfs, dfs):
        """Todo CPF/CNPJ deve ter uma transação de scoring."""
        n_customers = len(dfs["customer"])
        n_transactions = len(ext_dfs["fraud_transaction"])
        assert n_transactions == n_customers, (
            f"Esperado {n_customers} transações, obtido {n_transactions}"
        )

    def test_score_fk(self, ext_dfs):
        orphans = check_fk(
            ext_dfs["fraud_score"], "transaction_id",
            ext_dfs["fraud_transaction"], "transaction_id",
        )
        assert len(orphans) == 0

    def test_flags_fk(self, ext_dfs):
        if len(ext_dfs["fraud_flags"]) == 0:
            return
        orphans = check_fk(
            ext_dfs["fraud_flags"], "transaction_id",
            ext_dfs["fraud_transaction"], "transaction_id",
        )
        assert len(orphans) == 0

    def test_flags_only_for_alto(self, ext_dfs):
        """Flags só devem existir para transações com nível ALTO."""
        if len(ext_dfs["fraud_flags"]) == 0:
            return
        flag_txids = set(ext_dfs["fraud_flags"]["transaction_id"])
        alto_txids = set(
            ext_dfs["fraud_score"]
            .loc[ext_dfs["fraud_score"]["nivel_risco"] == "ALTO", "transaction_id"]
        )
        non_alto = flag_txids - alto_txids
        assert len(non_alto) == 0, f"{len(non_alto)} flags para transações não-ALTO"

    def test_fraud_entities_higher_scores(self, ext_dfs, dfs):
        """Score médio de fraudulentos deve ser maior que de normais."""
        inspection = dfs["inspection"]
        cu = dfs["consumer_unit"]
        fraud_cids = set(
            inspection.loc[
                inspection["resultado"] == "irregularidade_confirmada",
                "consumer_id",
            ]
        )
        fraud_cpfs = set(cu[cu["consumer_id"].isin(fraud_cids)]["cpf_cnpj"])

        tx = ext_dfs["fraud_transaction"]
        scores = ext_dfs["fraud_score"]
        merged = tx.merge(scores, on="transaction_id")

        fraud_mean = merged[merged["cpf_cnpj"].isin(fraud_cpfs)]["score"].mean()
        normal_mean = merged[~merged["cpf_cnpj"].isin(fraud_cpfs)]["score"].mean()

        assert fraud_mean > normal_mean, (
            f"Score médio de fraude ({fraud_mean:.1f}) deveria superar "
            f"score médio normal ({normal_mean:.1f})"
        )

    def test_nivel_risco_valid(self, ext_dfs):
        valid = {"BAIXO", "MEDIO", "ALTO"}
        actual = set(ext_dfs["fraud_score"]["nivel_risco"].unique())
        assert actual.issubset(valid)


# ---------------------------------------------------------------------------
# Legal Processes (Jusbrasil)
# ---------------------------------------------------------------------------

class TestLegal:

    def test_legal_process_non_empty(self, ext_dfs):
        assert len(ext_dfs["legal_process"]) > 0

    def test_party_fk(self, ext_dfs):
        orphans = check_fk(
            ext_dfs["legal_party"], "processo_id",
            ext_dfs["legal_process"], "processo_id",
        )
        assert len(orphans) == 0

    def test_movement_fk(self, ext_dfs):
        orphans = check_fk(
            ext_dfs["legal_movement"], "processo_id",
            ext_dfs["legal_process"], "processo_id",
        )
        assert len(orphans) == 0

    def test_every_process_has_parties(self, ext_dfs):
        proc_ids = set(ext_dfs["legal_process"]["processo_id"])
        party_proc_ids = set(ext_dfs["legal_party"]["processo_id"])
        missing = proc_ids - party_proc_ids
        assert len(missing) == 0, f"{len(missing)} processos sem partes"

    def test_area_enum_valid(self, ext_dfs):
        valid = {"CIVEL", "CRIMINAL", "TRABALHISTA", "ADMINISTRATIVO"}
        actual = set(ext_dfs["legal_process"]["area"].unique())
        assert actual.issubset(valid)

    def test_status_enum_valid(self, ext_dfs):
        valid = {"ATIVO", "ENCERRADO"}
        actual = set(ext_dfs["legal_process"]["status"].unique())
        assert actual.issubset(valid)


# ---------------------------------------------------------------------------
# Financial Debt
# ---------------------------------------------------------------------------

class TestFinancialDebt:

    def test_debt_non_empty(self, ext_dfs):
        assert len(ext_dfs["financial_debt"]) > 0

    def test_debt_columns(self, ext_dfs):
        expected = {"cpf_cnpj", "tipo_divida", "valor", "status", "data_inscricao"}
        assert expected.issubset(set(ext_dfs["financial_debt"].columns))

    def test_debt_tipo_valid(self, ext_dfs):
        valid = {"TRIBUTARIA", "PREVIDENCIARIA", "MULTA", "OUTROS"}
        actual = set(ext_dfs["financial_debt"]["tipo_divida"].unique())
        assert actual.issubset(valid)

    def test_debt_status_valid(self, ext_dfs):
        valid = {"ATIVA", "PARCELADA", "QUITADA"}
        actual = set(ext_dfs["financial_debt"]["status"].unique())
        assert actual.issubset(valid)

    def test_positive_values(self, ext_dfs):
        negatives = ext_dfs["financial_debt"][ext_dfs["financial_debt"]["valor"] <= 0]
        assert len(negatives) == 0, f"{len(negatives)} dívidas com valor ≤ 0"


# ---------------------------------------------------------------------------
# OSINT Events
# ---------------------------------------------------------------------------

class TestOsint:

    def test_osint_non_empty(self, ext_dfs):
        assert len(ext_dfs["osint_events"]) > 0

    def test_osint_columns(self, ext_dfs):
        expected = {
            "event_id", "cpf_cnpj", "fonte", "titulo",
            "descricao", "data", "sentimento",
        }
        assert expected.issubset(set(ext_dfs["osint_events"].columns))

    def test_sentimento_valid(self, ext_dfs):
        valid = {"NEGATIVO", "NEUTRO", "POSITIVO"}
        actual = set(ext_dfs["osint_events"]["sentimento"].unique())
        assert actual.issubset(valid)

    def test_fonte_valid(self, ext_dfs):
        valid = {"NEWS", "BLOG", "SOCIAL", "DIARIO_OFICIAL"}
        actual = set(ext_dfs["osint_events"]["fonte"].unique())
        assert actual.issubset(valid)
