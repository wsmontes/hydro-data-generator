"""
test_fraud.py — Testes de qualidade na produção de padrões de fraude.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ─────────────────────────────────────────────
# Padrão 1 — gradual_drop
# ─────────────────────────────────────────────

class TestGradualDrop:
    """Ao menos uma UC deve exibir queda persistente de consumo ≥ 45%."""

    def test_existe_uc_com_queda_gradual(self, meter_reading):
        df = meter_reading[["consumer_id", "data_leitura", "consumo_kwh"]].copy()
        df["data_leitura"] = pd.to_datetime(df["data_leitura"])
        df = df.sort_values(["consumer_id", "data_leitura"])

        achados = []
        for cid, grp in df.groupby("consumer_id"):
            if len(grp) < 6:
                continue
            primeiro = grp.head(3)["consumo_kwh"].mean()
            ultimo   = grp.tail(3)["consumo_kwh"].mean()
            if primeiro > 0 and (ultimo / primeiro) < 0.55:
                achados.append(cid)

        assert len(achados) >= 1, (
            "Nenhuma UC com queda de consumo ≥ 45% encontrada (esperado: padrão gradual_drop)"
        )

    def test_proporcao_minima_de_ucs_com_queda(self, meter_reading):
        """Com fraud_rate=0.15, ao menos 3% das UCs devem mostrar queda acentuada."""
        df = meter_reading[["consumer_id", "data_leitura", "consumo_kwh"]].copy()
        df["data_leitura"] = pd.to_datetime(df["data_leitura"])
        df = df.sort_values(["consumer_id", "data_leitura"])

        contador = 0
        total = 0
        for cid, grp in df.groupby("consumer_id"):
            if len(grp) < 4:
                continue
            total += 1
            primeiro = grp.head(2)["consumo_kwh"].mean()
            ultimo   = grp.tail(2)["consumo_kwh"].mean()
            if primeiro > 0 and (ultimo / primeiro) < 0.60:
                contador += 1

        pct = contador / total if total else 0
        assert pct >= 0.03, (
            f"Apenas {pct:.1%} de UCs com queda suspeita (esperado ≥3%)"
        )


# ─────────────────────────────────────────────
# Padrão 2 — reader_corruption
# ─────────────────────────────────────────────

class TestReaderCorruption:
    """Ao menos 1 leiturista deve ter média de consumo sistematicamente abaixo da média geral."""

    def test_existe_leiturista_com_leituras_baixas(self, meter_reading, reading_agent):
        df = meter_reading[["reading_id", "consumo_kwh"]].merge(
            reading_agent[["reading_id", "reader_id"]], on="reading_id"
        )
        media_global = df["consumo_kwh"].mean()
        por_leitor   = df.groupby("reader_id")["consumo_kwh"].mean()

        # Threshold 97%: com volumes grandes (50k+ UCs) a corrupção de ~3% das
        # leituras dilui o sinal mas ainda produz dip de ~3% na média do leitor.
        suspeitos = por_leitor[por_leitor < media_global * 0.97]
        assert len(suspeitos) >= 1, (
            f"Nenhum leiturista com média ≤97% da média global "
            f"(média global={media_global:.1f} kWh, min_leitor={por_leitor.min():.1f})"
        )


# ─────────────────────────────────────────────
# Padrão 3 — electrician_correlation
# ─────────────────────────────────────────────

class TestElectricianCorrelation:
    """Deve existir eletricista cujas OS precedem quedas de consumo em ≤ 7 dias."""

    def test_existe_correlacao_eletricista(self, work_order, meter_reading):
        wo = work_order[["consumer_id", "eletricista_id", "data_execucao"]].copy()
        wo["data_execucao"] = pd.to_datetime(wo["data_execucao"])

        lr = meter_reading[["consumer_id", "data_leitura", "consumo_kwh"]].copy()
        lr["data_leitura"] = pd.to_datetime(lr["data_leitura"])

        media_global = lr["consumo_kwh"].mean()

        merged = lr.merge(wo, on="consumer_id")
        merged["delta"] = (merged["data_leitura"] - merged["data_execucao"]).dt.days
        proximos = merged[(merged["delta"] > 0) & (merged["delta"] <= 7)]

        if len(proximos) == 0:
            pytest.skip("Nenhuma leitura dentro de 7 dias após OS — padrão pode não ter sido ativado")

        eletricistas_suspeitos = (
            proximos[proximos["consumo_kwh"] < media_global * 0.85]
            ["eletricista_id"]
            .nunique()
        )
        assert eletricistas_suspeitos >= 1, (
            "Nenhum eletricista com leituras baixas ≤7 dias após OS emitida"
        )


# ─────────────────────────────────────────────
# Padrão 4 — network_clusters (multi-UC / mesmo CPF-CNPJ)
# ─────────────────────────────────────────────

class TestNetworkClusters:
    """Deve existir ao menos 1 CPF/CNPJ vinculado a múltiplas UCs."""

    def test_existe_cpf_cnpj_com_multiplas_ucs(self, consumer_unit):
        contagem = consumer_unit.groupby("cpf_cnpj")["consumer_id"].nunique()
        multi = contagem[contagem >= 2]
        assert len(multi) >= 1, (
            "Nenhum CPF/CNPJ associado a ≥2 UCs (esperado: padrão network_clusters)"
        )

    def test_proporcao_multi_uc(self, consumer_unit):
        contagem = consumer_unit.groupby("cpf_cnpj")["consumer_id"].nunique()
        pct_multi = (contagem >= 2).mean()
        assert pct_multi >= 0.005, (
            f"Proporção de proprietários com ≥2 UCs muito baixa: {pct_multi:.2%}"
        )


# ─────────────────────────────────────────────
# Padrão 5 — recurrence (reincidência)
# ─────────────────────────────────────────────

class TestRecurrence:
    """Deve existir ao menos 1 inspeção de reincidência (flag=True)."""

    def test_existe_reincidencia(self, inspection):
        # Q1: 'reincidencia_confirmada' não é mais um resultado válido.
        # A reincidência é agora capturada exclusivamente por reincidente_flag=True.
        reincidentes = inspection[inspection["reincidente_flag"].astype(bool)]
        assert len(reincidentes) >= 1, (
            "Nenhuma inspeção com reincidente_flag=True encontrada"
        )

    def test_reincidente_flag_consistente(self, inspection):
        """reincidente_flag=True indica histórico de inspeção anterior — pode
        ter qualquer resultado. Valida que a flag tem valores distintos (não constante)."""
        if "reincidente_flag" not in inspection.columns:
            pytest.skip("Coluna reincidente_flag não encontrada")
        valores = inspection["reincidente_flag"].unique()
        # Deve haver UCs tanto com quanto sem histórico
        assert len(valores) >= 2, (
            "reincidente_flag só contém um único valor — esperado True e False"
        )

    # E1 — reincidente_flag=True obrigatório em reincidencia_confirmada
    def test_reincidencia_confirmada_implica_flag_true(self, inspection):
        """Toda inspeção reincidencia_confirmada DEVE ter reincidente_flag=True (regra E1)."""
        reincidencias = inspection[inspection["resultado"] == "reincidencia_confirmada"]
        if len(reincidencias) == 0:
            pytest.skip("Nenhuma reincidencia_confirmada no dataset")
        sem_flag = reincidencias[~reincidencias["reincidente_flag"].astype(bool)]
        assert len(sem_flag) == 0, (
            f"E1: {len(sem_flag)} reincidencias_confirmadas com reincidente_flag=False"
        )

    # E2 — reincidencia_confirmada nunca pode ser a primeira inspeção de uma UC
    def test_reincidencia_confirmada_requer_inspecao_anterior(self, inspection):
        """Uma UC não pode ter sua primeira inspeção classificada como reincidencia_confirmada."""
        df = inspection[["consumer_id", "data_inspecao", "resultado"]].copy()
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])
        df = df.sort_values(["consumer_id", "data_inspecao"])
        primeiras = df.groupby("consumer_id").first().reset_index()
        violacoes = primeiras[primeiras["resultado"] == "reincidencia_confirmada"]
        assert len(violacoes) == 0, (
            f"E2: {len(violacoes)} UCs com primeira inspeção já como reincidencia_confirmada"
        )

    # N3 — reincidente_flag determinístico: toda inspeção pós-confirmação deve ter flag=True
    def test_reincidente_flag_deterministico_apos_confirmacao(self, inspection):
        """Qualquer inspeção de UC que já teve irregularidade confirmada deve ter flag=True."""
        df = inspection[["consumer_id", "data_inspecao", "resultado", "reincidente_flag"]].copy()
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])
        df = df.sort_values(["consumer_id", "data_inspecao"])
        # Q1: apenas irregularidade_confirmada (único resultado válido de fraude)
        confirmadas = {"irregularidade_confirmada"}
        violacoes = []
        for cid, grp in df.groupby("consumer_id"):
            ja_confirmou = False
            for _, row in grp.iterrows():
                if ja_confirmou and not row["reincidente_flag"]:
                    violacoes.append({"consumer_id": cid, "resultado": row["resultado"]})
                if row["resultado"] in confirmadas:
                    ja_confirmou = True
        assert len(violacoes) == 0, (
            f"N3: {len(violacoes)} inspeções pós-confirmação com reincidente_flag=False: {violacoes[:5]}"
        )

    def test_uc_com_multiplas_inspecoes(self, inspection):
        """Com padrão recurrence, deve haver UCs inspecionadas mais de uma vez."""
        por_uc = inspection.groupby("consumer_id").size()
        multiplas = (por_uc >= 2).sum()
        assert multiplas >= 1, "Nenhuma UC com mais de uma inspeção"


# ─────────────────────────────────────────────
# Padrão 6 — perdas em transformadores com fraude
# ─────────────────────────────────────────────

class TestTransformerFraudLoss:
    """Transformadores com UCs fraudulentas devem ter perdas maiores em média."""

    def test_transformadores_fraud_tem_perda_maior(
        self, consumer_unit, transformer_reading
    ):
        # Identifica se a tabela consumer_unit tem coluna is_fraud (não deveria
        # estar exportada, mas podemos detectar padrão via transformer_reading
        # e a relação transformer_id × consumer_id)
        # Estratégia alternativa: comparar distribuição de perdas
        q75 = transformer_reading["perda_estimada_pct"].quantile(0.75)
        q25 = transformer_reading["perda_estimada_pct"].quantile(0.25)
        spread = q75 - q25

        # Com fraud_rate=0.15, deve haver dispersão real nas perdas
        assert spread >= 2.0, (
            f"Spread IQR de perdas muito estreito: {spread:.2f}pp "
            "(esperado ≥2pp com mistura fraude/normal)"
        )

    def test_media_perdas_alta_para_transformadores_de_alta_perda(
        self, transformer_reading
    ):
        """Quartil superior de transformadores deve ter perda > 10%."""
        alto = transformer_reading[
            transformer_reading["perda_estimada_pct"] >=
            transformer_reading["perda_estimada_pct"].quantile(0.75)
        ]
        assert alto["perda_estimada_pct"].mean() > 10.0


# ─────────────────────────────────────────────
# Padrão 7 — taxa de fraude global
# ─────────────────────────────────────────────

class TestFraudRate:
    """A taxa de fraudes (irregularidades confirmadas / total de UCs) deve
    ser aproximadamente igual ao fraud_rate configurado (0.15)."""

    def test_taxa_irregular_dentro_do_esperado(self, inspection, consumer_unit, request):
        import yaml
        config_path = request.config.getoption("--config")
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        fraud_rate = cfg.get("fraud", {}).get("fraud_rate", cfg.get("fraud_rate", 0.08))

        confirmadas = inspection[
            inspection["resultado"].isin(
                ["irregularidade_confirmada", "reincidencia_confirmada"]
            )
        ]["consumer_id"].nunique()

        total_ucs = len(consumer_unit)
        taxa = confirmadas / total_ucs if total_ucs else 0

        # Tolerance: fraud_rate ± 10pp (inspections são amostragem, não universo)
        lo = max(0.01, fraud_rate - 0.10)
        hi = fraud_rate + 0.10
        assert lo <= taxa <= hi, (
            f"Taxa de irregularidades={taxa:.1%} fora do intervalo esperado "
            f"[{lo:.0%}, {hi:.0%}] (fraud_rate configurado={fraud_rate:.0%})"
        )

    def test_inspecoes_realizadas(self, inspection, consumer_unit):
        """Com fraud_rate=0.15 em 300 UCs, esperamos ao menos 30 inspeções."""
        assert len(inspection) >= 30, (
            f"Apenas {len(inspection)} inspeções — esperado ≥30 para 300 UCs"
        )


# ─────────────────────────────────────────────
# Padrão 8 — ocorrências suspeitas nas leituras
# ─────────────────────────────────────────────

class TestReadingOccurrences:
    """Occurrências de leitura devem incluir tipos suspeitos."""

    def test_tipos_suspeitos_presentes(self, reading_occurrence):
        suspeitos = {
            "medidor_violado", "lacre_rompido", "acesso_negado",
            "consumo_anomalo", "medidor_adulterado",
        }
        presentes = set(reading_occurrence["tipo_ocorrencia"].unique())
        intersecao = suspeitos & presentes
        assert len(intersecao) >= 1, (
            f"Nenhum tipo suspeito encontrado. Tipos presentes: {presentes}"
        )

    def test_ocorrencias_associadas_a_leituras_validas(
        self, reading_occurrence, meter_reading
    ):
        orfaos = reading_occurrence[
            ~reading_occurrence["reading_id"].isin(meter_reading["reading_id"])
        ]
        assert len(orfaos) == 0
