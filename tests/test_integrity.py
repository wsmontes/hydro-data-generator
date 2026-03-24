"""
test_integrity.py — Integridade referencial e coerência entre tabelas.
"""

from __future__ import annotations

import pandas as pd
import pytest


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def check_fk(child: pd.DataFrame, child_col: str,
             parent: pd.DataFrame, parent_col: str) -> pd.Series:
    """Retorna registros do filho cujos valores NÃO existem no pai."""
    valid = set(parent[parent_col].dropna().unique())
    mask  = child[child_col].notna() & ~child[child_col].isin(valid)
    return child[mask]


# ─────────────────────────────────────────────
# Chaves Estrangeiras — consumer_unit
# ─────────────────────────────────────────────

class TestFKConsumerUnit:

    def test_cpf_cnpj_em_customer(self, consumer_unit, customer):
        orfaos = check_fk(consumer_unit, "cpf_cnpj", customer, "cpf_cnpj")
        assert len(orfaos) == 0, (
            f"{len(orfaos)} consumer_unit com cpf_cnpj inexistente em customer"
        )

    def test_transformador_em_transformer(self, consumer_unit, transformer):
        orfaos = check_fk(consumer_unit, "transformador_id", transformer, "transformer_id")
        assert len(orfaos) == 0, (
            f"{len(orfaos)} consumer_unit com transformador_id inexistente"
        )

    def test_endereco_em_address(self, consumer_unit, address):
        orfaos = check_fk(consumer_unit, "endereco_id", address, "endereco_id")
        assert len(orfaos) == 0

    def test_cnae_em_economic_activity(self, consumer_unit, economic_activity):
        """Apenas registros não-nulos precisam existir em economic_activity."""
        uc_com_cnae = consumer_unit[consumer_unit["cnae"].notna()]
        orfaos = check_fk(uc_com_cnae, "cnae", economic_activity, "cnae")
        assert len(orfaos) == 0, (
            f"{len(orfaos)} UCs com CNAE inválido: {orfaos['cnae'].unique().tolist()}"
        )


# ─────────────────────────────────────────────
# Chaves Estrangeiras — meter_reading
# ─────────────────────────────────────────────

class TestFKMeterReading:

    def test_consumer_id_em_consumer_unit(self, meter_reading, consumer_unit):
        orfaos = check_fk(meter_reading, "consumer_id", consumer_unit, "consumer_id")
        assert len(orfaos) == 0

    def test_cobertura_total_de_ucs(self, meter_reading, consumer_unit):
        """Toda UC ativa deve ter ao menos uma leitura."""
        ativas = set(
            consumer_unit[consumer_unit["status"] == "ativo"]["consumer_id"]
        )
        com_leitura = set(meter_reading["consumer_id"].unique())
        sem_leitura = ativas - com_leitura
        assert len(sem_leitura) == 0, (
            f"{len(sem_leitura)} UCs ativas sem nenhuma leitura"
        )

    def test_no_maximo_uma_leitura_por_uc_mes(self, meter_reading):
        df = meter_reading.copy()
        df["ano_mes"] = pd.to_datetime(df["data_leitura"]).dt.to_period("M")
        dupes = df.groupby(["consumer_id", "ano_mes"]).size()
        multiplas = (dupes > 1).sum()
        assert multiplas == 0, (
            f"{multiplas} pares (UC × mês) com mais de uma leitura"
        )


# ─────────────────────────────────────────────
# Chaves Estrangeiras — reading_agent
# ─────────────────────────────────────────────

class TestFKReadingAgent:

    def test_reading_id_em_meter_reading(self, reading_agent, meter_reading):
        orfaos = check_fk(reading_agent, "reading_id", meter_reading, "reading_id")
        assert len(orfaos) == 0

    def test_reader_id_em_meter_reader(self, reading_agent, meter_reader):
        orfaos = check_fk(reading_agent, "reader_id", meter_reader, "reader_id")
        assert len(orfaos) == 0

    def test_cobertura_1_para_1_com_meter_reading(self, reading_agent, meter_reading):
        """Toda leitura deve ter exatamente 1 agente registrado."""
        covered   = set(reading_agent["reading_id"].unique())
        all_reads = set(meter_reading["reading_id"].unique())
        sem_agente = all_reads - covered
        assert len(sem_agente) == 0, (
            f"{len(sem_agente)} leituras sem reading_agent"
        )


# ─────────────────────────────────────────────
# Chaves Estrangeiras — reading_occurrence
# ─────────────────────────────────────────────

class TestFKReadingOccurrence:

    def test_reading_id_em_meter_reading(self, reading_occurrence, meter_reading):
        orfaos = check_fk(reading_occurrence, "reading_id", meter_reading, "reading_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# Chaves Estrangeiras — meter_image
# ─────────────────────────────────────────────

class TestFKMeterImage:

    def test_reading_id_em_meter_reading(self, meter_image, meter_reading):
        orfaos = check_fk(meter_image, "reading_id", meter_reading, "reading_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# Chaves Estrangeiras — work_order
# ─────────────────────────────────────────────

class TestFKWorkOrder:

    def test_consumer_id_em_consumer_unit(self, work_order, consumer_unit):
        orfaos = check_fk(work_order, "consumer_id", consumer_unit, "consumer_id")
        assert len(orfaos) == 0

    def test_eletricista_em_electrician(self, work_order, electrician):
        orfaos = check_fk(work_order, "eletricista_id", electrician, "eletricista_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# Chaves Estrangeiras — inspection
# ─────────────────────────────────────────────

class TestFKInspection:

    def test_consumer_id_em_consumer_unit(self, inspection, consumer_unit):
        orfaos = check_fk(inspection, "consumer_id", consumer_unit, "consumer_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# E5 — toda OS de vistoria_fraude deve ter inspeção correspondente
# ─────────────────────────────────────────────

class TestVistoriaFraudeInspection:

    def test_vistoria_fraude_tem_inspecao(self, work_order, inspection):
        """Toda OS de tipo vistoria_fraude deve gerar ao menos uma inspeção na UC."""
        vistoria_ucs = set(
            work_order.loc[
                work_order["tipo_servico"] == "vistoria_fraude", "consumer_id"
            ].unique()
        )
        inspecionadas = set(inspection["consumer_id"].unique())
        sem_inspecao = vistoria_ucs - inspecionadas
        assert len(sem_inspecao) == 0, (
            f"{len(sem_inspecao)} UCs com OS vistoria_fraude sem inspeção: "
            f"{sorted(sem_inspecao)[:10]}"
        )


# ─────────────────────────────────────────────
# Chaves Estrangeiras — declared_load
# ─────────────────────────────────────────────

class TestFKDeclaredLoad:

    def test_consumer_id_em_consumer_unit(self, declared_load, consumer_unit):
        orfaos = check_fk(declared_load, "consumer_id", consumer_unit, "consumer_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# Chaves Estrangeiras — external_property_data
# ─────────────────────────────────────────────

class TestFKExternalPropertyData:

    def test_consumer_id_em_consumer_unit(self, external_property_data, consumer_unit):
        orfaos = check_fk(external_property_data, "consumer_id", consumer_unit, "consumer_id")
        assert len(orfaos) == 0


# ─────────────────────────────────────────────
# Chaves Estrangeiras — transformer_reading
# ─────────────────────────────────────────────

class TestFKTransformerReading:

    def test_transformer_id_em_transformer(self, transformer_reading, transformer):
        orfaos = check_fk(transformer_reading, "transformer_id", transformer, "transformer_id")
        assert len(orfaos) == 0

    def test_todos_transformadores_tem_leituras(self, transformer_reading, transformer):
        com_leitura = set(transformer_reading["transformer_id"].unique())
        todos       = set(transformer["transformer_id"].unique())
        sem_leitura = todos - com_leitura
        assert len(sem_leitura) == 0, (
            f"{len(sem_leitura)} transformadores sem nenhuma leitura"
        )


# ─────────────────────────────────────────────
# Coerência temporal
# ─────────────────────────────────────────────

class TestTemporalCoherence:

    def test_data_leitura_apos_data_ligacao(self, meter_reading, consumer_unit):
        df = meter_reading.merge(
            consumer_unit[["consumer_id", "data_ligacao"]], on="consumer_id"
        )
        df["data_leitura"]  = pd.to_datetime(df["data_leitura"])
        df["data_ligacao"]  = pd.to_datetime(df["data_ligacao"])
        anteriores = df[df["data_leitura"] < df["data_ligacao"]]
        assert len(anteriores) == 0, (
            f"{len(anteriores)} leituras anteriores à data de ligação da UC"
        )

    def test_work_order_apos_data_ligacao(self, work_order, consumer_unit):
        df = work_order[["consumer_id", "data_execucao"]].merge(
            consumer_unit[["consumer_id", "data_ligacao"]], on="consumer_id"
        )
        df["data_execucao"] = pd.to_datetime(df["data_execucao"])
        df["data_ligacao"]  = pd.to_datetime(df["data_ligacao"])
        anteriores = df[df["data_execucao"] < df["data_ligacao"]]
        assert len(anteriores) == 0

    def test_inspection_apos_data_ligacao(self, inspection, consumer_unit):
        df = inspection.merge(
            consumer_unit[["consumer_id", "data_ligacao"]], on="consumer_id"
        )
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])
        df["data_ligacao"]  = pd.to_datetime(df["data_ligacao"])
        anteriores = df[df["data_inspecao"] < df["data_ligacao"]]
        assert len(anteriores) == 0


# ─────────────────────────────────────────────
# Coerência de conteúdo cruzado
# ─────────────────────────────────────────────

class TestCrossContentCoherence:

    def test_tipo_consumidor_vs_tipo_ligacao(self, consumer_unit):
        """Validação de perfil × ligação condensada: residencial nunca AT."""
        violacoes = consumer_unit[
            (consumer_unit["tipo_consumidor"] == "residencial") &
            (consumer_unit["tipo_ligacao"] == "AT")
        ]
        assert len(violacoes) == 0

    def test_customer_tipo_coerente_com_documento(self, customer):
        """PJ deve ter CNPJ (contém '/'), PF deve ter CPF (não contém '/')."""
        pj_sem_cnpj = customer[
            (customer["tipo"] == "PJ") &
            (~customer["cpf_cnpj"].str.contains("/", na=False))
        ]
        pf_com_cnpj = customer[
            (customer["tipo"] == "PF") &
            (customer["cpf_cnpj"].str.contains("/", na=False))
        ]
        assert len(pj_sem_cnpj) == 0, f"PJ sem CNPJ: {len(pj_sem_cnpj)}"
        assert len(pf_com_cnpj) == 0, f"PF com CNPJ: {len(pf_com_cnpj)}"

    def test_reading_id_unico_em_meter_reading(self, meter_reading):
        dupes = meter_reading[meter_reading.duplicated("reading_id", keep=False)]
        assert len(dupes) == 0, f"{len(dupes)} reading_id duplicados"

    def test_consumer_id_unico_em_consumer_unit(self, consumer_unit):
        dupes = consumer_unit[consumer_unit.duplicated("consumer_id", keep=False)]
        assert len(dupes) == 0
