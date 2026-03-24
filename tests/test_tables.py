"""
test_tables.py — Testes de qualidade e regras de negócio por tabela.
"""

from __future__ import annotations

import re

import pandas as pd
import pytest

# Regex de documentos brasileiros
CPF_RE  = re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")
CNPJ_RE = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")


# ─────────────────────────────────────────────
# customer
# ─────────────────────────────────────────────

class TestCustomer:

    def test_cpf_formato_valido(self, customer):
        pf = customer[customer["tipo"] == "PF"]["cpf_cnpj"]
        invalidos = pf[~pf.str.match(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")]
        assert len(invalidos) == 0, f"CPFs com formato inválido:\n{invalidos.head()}"

    def test_cnpj_formato_valido(self, customer):
        pj = customer[customer["tipo"] == "PJ"]["cpf_cnpj"]
        invalidos = pj[~pj.str.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")]
        assert len(invalidos) == 0, f"CNPJs com formato inválido:\n{invalidos.head()}"

    def test_sem_duplicatas_cpf_cnpj(self, customer):
        dupes = customer[customer.duplicated("cpf_cnpj", keep=False)]
        assert len(dupes) == 0, f"CPF/CNPJs duplicados na tabela customer:\n{dupes}"

    def test_tipo_somente_pf_pj(self, customer):
        tipos_validos = {"PF", "PJ"}
        tipos_presentes = set(customer["tipo"].unique())
        assert tipos_presentes.issubset(tipos_validos)

    def test_nome_nao_nulo(self, customer):
        assert customer["nome"].isna().sum() == 0

    def test_ambos_tipos_presentes(self, customer):
        assert "PF" in customer["tipo"].values
        assert "PJ" in customer["tipo"].values

    def test_pf_maioria(self, customer):
        pct_pf = (customer["tipo"] == "PF").mean()
        assert pct_pf >= 0.70, f"PF deveria ser ≥70%; encontrado {pct_pf:.1%}"

    # N1 — clientes PF não devem ter honoríficos
    def test_sem_honorificos_em_clientes_pf(self, customer):
        """Nomes de clientes PF refletem cadastro da Receita — sem honoríficos."""
        HONORIFIC_RE = r"(?:Sr\.|Sra\.|Dr\.|Dra\.|Prof\.|Profa\.|Srta\.)"
        pf = customer[customer["tipo"] == "PF"]
        violacoes = pf[pf["nome"].str.contains(HONORIFIC_RE, regex=True, na=False)]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} clientes PF com honoríficos: "
            f"{violacoes['nome'].tolist()[:5]}"
        )

    # Q2 — todo cliente deve estar vinculado a ao menos 1 UC (sem órfãos)
    def test_todos_clientes_tem_uc(self, customer, consumer_unit):
        """Todos os CPF/CNPJs em customer.csv devem aparecer em ao menos 1 UC."""
        uc_cpfs = set(consumer_unit["cpf_cnpj"].dropna())
        cust_cpfs = set(customer["cpf_cnpj"].dropna())
        orfaos = cust_cpfs - uc_cpfs
        assert len(orfaos) == 0, (
            f"{len(orfaos)} clientes sem nenhuma UC associada: {sorted(orfaos)[:5]}"
        )

    # Q4 — razões sociais PJ devem ter sufixo legal
    def test_pj_nomes_tem_sufixo_legal(self, customer):
        """Nomes de PJ devem conter sufixo legal: Ltda., S.A., S/A, ME, EI, 'e Filhos', etc."""
        SUFIXOS = ("ltda", "s.a", "s/a", "- me", "- ei", "filhos", "& cia", "cia.", "associados")
        pj = customer[customer["tipo"] == "PJ"]
        sem_sufixo = pj[
            ~pj["nome"].str.lower().str.contains("|".join(re.escape(s) for s in SUFIXOS), regex=True)
        ]
        assert len(sem_sufixo) == 0, (
            f"{len(sem_sufixo)} PJs sem sufixo legal: {sem_sufixo['nome'].tolist()[:10]}"
        )


# ─────────────────────────────────────────────
# address
# ─────────────────────────────────────────────

class TestAddress:

    def test_sem_cidades_genericas(self, address):
        genericas = address["cidade"].str.contains(r"Cidade [A-Z]", regex=True)
        assert genericas.sum() == 0, "Encontradas cidades genéricas ('Cidade A', etc.)"

    def test_estado_presente(self, address):
        assert "estado" in address.columns
        assert address["estado"].isna().sum() == 0

    def test_renda_positiva(self, address):
        assert (address["renda_media_regiao"] > 0).all()

    def test_bairro_sem_ingles(self, address):
        ingles = address["bairro"].str.contains(r"\bPark\b|\bDistrict\b", regex=True)
        assert ingles.sum() == 0, "Bairros com nomes em inglês encontrados"


# ─────────────────────────────────────────────
# consumer_unit
# ─────────────────────────────────────────────

class TestConsumerUnit:

    def test_sem_residencial_com_at(self, consumer_unit):
        violacoes = consumer_unit[
            (consumer_unit["tipo_consumidor"] == "residencial") &
            (consumer_unit["tipo_ligacao"] == "AT")
        ]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} consumidores residenciais com ligação AT"
        )

    def test_sem_residencial_com_cnpj(self, consumer_unit):
        violacoes = consumer_unit[
            (consumer_unit["tipo_consumidor"] == "residencial") &
            (consumer_unit["cpf_cnpj"].str.contains("/", na=False))
        ]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} consumidores residenciais com CNPJ"
        )

    # N2 — residencial é sempre Grupo B1 (ANEEL): mono ou bifásico
    def test_residencial_nao_trifasico(self, consumer_unit):
        """Residencial nunca pode ter ligação trifásica ou AT."""
        violacoes = consumer_unit[
            (consumer_unit["tipo_consumidor"] == "residencial") &
            (consumer_unit["tipo_ligacao"].isin(["trifasico", "AT"]))
        ]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} residenciais com ligação trifásico/AT: "
            f"{violacoes[['consumer_id','tipo_ligacao']].head().to_dict('records')}"
        )

    def test_sem_residencial_com_cnae(self, consumer_unit):
        violacoes = consumer_unit[
            (consumer_unit["tipo_consumidor"] == "residencial") &
            (consumer_unit["cnae"].notna())
        ]
        assert len(violacoes) == 0

    def test_nao_residencial_tem_cnae(self, consumer_unit):
        """Comercial, industrial e rural sempre têm CNAE."""
        nao_res = consumer_unit[consumer_unit["tipo_consumidor"] != "residencial"]
        sem_cnae = nao_res[nao_res["cnae"].isna()]
        assert len(sem_cnae) == 0, (
            f"{len(sem_cnae)} UCs não-residenciais sem CNAE"
        )

    def test_perfil_consumo_nao_exportado(self, consumer_unit):
        assert "perfil_consumo" not in consumer_unit.columns

    def test_is_fraud_nao_exportado(self, consumer_unit):
        assert "is_fraud" not in consumer_unit.columns

    def test_industrial_somente_tri_ou_at(self, consumer_unit):
        ind = consumer_unit[consumer_unit["tipo_consumidor"] == "industrial"]
        invalidos = ind[~ind["tipo_ligacao"].isin(["trifasico", "AT"])]
        assert len(invalidos) == 0, (
            f"Industrial com ligação inválida: {invalidos['tipo_ligacao'].unique()}"
        )

    def test_installation_number_unico(self, consumer_unit):
        assert consumer_unit["installation_number"].nunique() == len(consumer_unit)

    def test_installation_number_sem_padrao_aritmetico(self, consumer_unit):
        """Números de instalação não devem ter diferença constante (anti-INST-00010003)."""
        nums = consumer_unit["installation_number"].astype(int).sort_values().values
        diffs = set((nums[1:] - nums[:-1]).tolist())
        assert len(diffs) > 5, "installation_number parece sequencial/aritmético"

    def test_status_validos(self, consumer_unit):
        validos = {"ativo", "suspenso", "cortado"}
        presentes = set(consumer_unit["status"].str.lower().unique())
        assert presentes.issubset(validos)

    def test_tipo_consumidor_validos(self, consumer_unit):
        validos = {"residencial", "comercial", "industrial", "rural"}
        presentes = set(consumer_unit["tipo_consumidor"].unique())
        assert presentes.issubset(validos)

    def test_residencial_maioria(self, consumer_unit):
        pct = (consumer_unit["tipo_consumidor"] == "residencial").mean()
        assert pct >= 0.40, f"Residencial deveria ser maioria; encontrado {pct:.1%}"

    # R3 — todo transformador deve ter ao menos 1 UC atribuída
    def test_todo_transformador_tem_uc(self, consumer_unit, transformer):
        """Nenhum transformador pode ficar sem UCs (nó isolado no grafo)."""
        ucs_por_tr = set(consumer_unit["transformador_id"].dropna())
        trs_sem_uc = set(transformer["transformer_id"]) - ucs_por_tr
        assert len(trs_sem_uc) == 0, (
            f"{len(trs_sem_uc)} transformadores sem nenhuma UC: {sorted(trs_sem_uc)[:5]}"
        )


# ─────────────────────────────────────────────
# meter_reading
# ─────────────────────────────────────────────

class TestMeterReading:

    def test_demanda_nula_para_monofasico_bifasico(self, meter_reading, consumer_unit):
        """UCs com ligação monofásica ou bifásica não devem ter demanda_kw preenchida."""
        grupo_b = consumer_unit[
            ~consumer_unit["tipo_ligacao"].isin(["trifasico", "AT"])
        ]["consumer_id"]
        leituras_b = meter_reading[meter_reading["consumer_id"].isin(grupo_b)]
        com_demanda = leituras_b["demanda_kw"].notna().sum()
        assert com_demanda == 0, (
            f"{com_demanda} leituras de mono/bifásico têm demanda_kw preenchida"
        )

    def test_demanda_preenchida_para_at(self, meter_reading, consumer_unit):
        grupo_a = consumer_unit[consumer_unit["tipo_ligacao"] == "AT"]["consumer_id"]
        if len(grupo_a) == 0:
            pytest.skip("Nenhuma UC com ligação AT no dataset de teste")
        leituras_a = meter_reading[meter_reading["consumer_id"].isin(grupo_a)]
        sem_demanda = leituras_a["demanda_kw"].isna().sum()
        assert sem_demanda == 0, (
            f"{sem_demanda} leituras de AT sem demanda_kw"
        )

    # MR-1 — UCs trifásicas devem ter demanda_kw em todas as leituras
    def test_demanda_preenchida_para_trifasico(self, meter_reading, consumer_unit):
        """Todo UC com ligação trifásica deve ter demanda_kw em todas as suas leituras."""
        tri_ids = consumer_unit[consumer_unit["tipo_ligacao"] == "trifasico"]["consumer_id"]
        if len(tri_ids) == 0:
            pytest.skip("Nenhuma UC trifásica no dataset de teste")
        leituras_tri = meter_reading[meter_reading["consumer_id"].isin(tri_ids)]
        sem_demanda = leituras_tri["demanda_kw"].isna().sum()
        assert sem_demanda == 0, (
            f"{sem_demanda} leituras trifásicas sem demanda_kw"
        )

    # Q5/MR-1/S1 — demanda_kw deve estar em faixa realista por tipo de ligação
    def test_demanda_kw_industrial_realista(self, meter_reading, consumer_unit):
        """Demanda medida: AT → 135–550 kW; trifásico BT → 3–35 kW."""
        mr = meter_reading
        # AT: demanda contratada 150–500 kW ±10%
        at_ids = consumer_unit[consumer_unit["tipo_ligacao"] == "AT"]["consumer_id"]
        if len(at_ids) > 0:
            lat = mr[mr["consumer_id"].isin(at_ids) & mr["demanda_kw"].notna()]
            assert (lat["demanda_kw"] >= 135).all(), \
                f"{(lat['demanda_kw'] < 135).sum()} leituras AT com demanda_kw < 135 kW"
            assert (lat["demanda_kw"] <= 550).all(), \
                f"{(lat['demanda_kw'] > 550).sum()} leituras AT com demanda_kw > 550 kW"
        # Trifásico BT: demanda derivada do perfil de consumo → 3–35 kW
        tri_ids = consumer_unit[consumer_unit["tipo_ligacao"] == "trifasico"]["consumer_id"]
        if len(tri_ids) == 0:
            pytest.skip("Nenhuma UC trifásica BT no dataset de teste")
        ltri = mr[mr["consumer_id"].isin(tri_ids) & mr["demanda_kw"].notna()]
        assert (ltri["demanda_kw"] >= 3).all(), \
            f"{(ltri['demanda_kw'] < 3).sum()} leituras trifásico BT com demanda_kw < 3 kW"
        assert (ltri["demanda_kw"] <= 35).all(), \
            f"{(ltri['demanda_kw'] > 35).sum()} leituras trifásico BT com demanda_kw > 35 kW"

    def test_consumo_nao_negativo(self, meter_reading):
        negativos = (meter_reading["consumo_kwh"] < 0).sum()
        assert negativos == 0

    def test_dias_entre_leituras_intervalo(self, meter_reading):
        fora = meter_reading[
            (meter_reading["dias_entre_leituras"] < 27) |
            (meter_reading["dias_entre_leituras"] > 34)
        ]
        assert len(fora) == 0, f"{len(fora)} leituras com dias fora de [27,34]"

    def test_janela_faturamento(self, meter_reading):
        df = meter_reading.copy()
        df["gap"] = (
            pd.to_datetime(df["data_faturamento"]) - pd.to_datetime(df["data_leitura"])
        ).dt.days
        fora = df[(df["gap"] < 5) | (df["gap"] > 15)]
        assert len(fora) == 0, (
            f"{len(fora)} leituras com gap leitura→faturamento fora de [5,15] dias"
        )

    def test_consumo_normalizado_coerente(self, meter_reading):
        """consumo_normalizado_30d deve ser próximo de consumo * 30 / dias."""
        df = meter_reading.copy()
        df["expected_norm"] = (df["consumo_kwh"] * 30 / df["dias_entre_leituras"]).round(2)
        df["diff"] = (df["consumo_normalizado_30d"] - df["expected_norm"]).abs()
        acima = (df["diff"] > 1.0).sum()   # tolerância de R$1 por arredondamento
        assert acima == 0, f"{acima} linhas com consumo_normalizado inconsistente"

    def test_datas_no_periodo_configurado(self, meter_reading):
        datas = pd.to_datetime(meter_reading["data_leitura"])
        assert datas.min() >= pd.Timestamp("2023-01-01")
        assert datas.max() <= pd.Timestamp("2026-01-31")  # margem de 1 mês


# ─────────────────────────────────────────────
# transformer_reading
# ─────────────────────────────────────────────

class TestTransformerReading:

    # P2 — perda máxima realítica cap 15% (fraude alta) / 12% (fraude leve) / 7% (sem fraude)
    def test_perda_dentro_faixa_realista(self, transformer_reading):
        """Perdas entre 3% e 15% — capão definido em P2."""
        fora = transformer_reading[
            (transformer_reading["perda_estimada_pct"] < 2.5) |
            (transformer_reading["perda_estimada_pct"] > 15.0)
        ]
        assert len(fora) == 0, (
            f"{len(fora)} registros com perda fora de [2.5%, 15%]: "
            f"max={transformer_reading['perda_estimada_pct'].max():.2f}%"
        )

    def test_energia_total_maior_que_soma_consumidores(self, transformer_reading):
        """O transformador deve sempre registrar mais do que a soma dos consumidores."""
        violacoes = transformer_reading[
            transformer_reading["energia_total_kwh"] <= transformer_reading["soma_consumidores_kwh"]
        ]
        assert len(violacoes) == 0

    def test_colunas_presentes(self, transformer_reading):
        obrigatorias = {"reading_id", "transformer_id", "data",
                        "energia_total_kwh", "soma_consumidores_kwh", "perda_estimada_pct"}
        assert obrigatorias.issubset(set(transformer_reading.columns))


# ─────────────────────────────────────────────
# inspection
# ─────────────────────────────────────────────

class TestInspection:

    def test_sem_irregularidade_sem_tipo(self, inspection):
        violacoes = inspection[
            (inspection["resultado"] == "sem_irregularidade") &
            (inspection["tipo_irregularidade"].notna())
        ]
        assert len(violacoes) == 0

    def test_sem_irregularidade_sem_valor(self, inspection):
        violacoes = inspection[
            (inspection["resultado"] == "sem_irregularidade") &
            (inspection["valor_recuperado"] > 0)
        ]
        assert len(violacoes) == 0

    def test_confirmada_tem_tipo(self, inspection):
        confirmadas = inspection[
            inspection["resultado"] == "irregularidade_confirmada"
        ]
        sem_tipo = confirmadas[confirmadas["tipo_irregularidade"].isna()]
        assert len(sem_tipo) == 0

    def test_confirmada_tem_valor_positivo(self, inspection):
        confirmadas = inspection[
            inspection["resultado"] == "irregularidade_confirmada"
        ]
        sem_valor = confirmadas[confirmadas["valor_recuperado"] <= 0]
        assert len(sem_valor) == 0

    def test_inconcluso_valor_zero(self, inspection):
        violacoes = inspection[
            (inspection["resultado"] == "inconcluso") &
            (inspection["valor_recuperado"] > 0)
        ]
        assert len(violacoes) == 0

    # R1 — tipo_irregularidade só pode ser preenchido em irregularidade_confirmada
    def test_inconcluso_sem_tipo(self, inspection):
        """Resultado inconclusivo não permite identificar o tipo de irregularidade."""
        violacoes = inspection[
            (inspection["resultado"] == "inconcluso") &
            (inspection["tipo_irregularidade"].notna())
        ]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} inspeções inconclusas com tipo_irregularidade preenchido: "
            f"{violacoes[['toi_id','tipo_irregularidade']].head().to_dict('records')}"
        )

    def test_resultados_validos(self, inspection):
        # Q1: reincidencia_confirmada é domínio inválido — removido
        validos = {
            "irregularidade_confirmada", "sem_irregularidade", "inconcluso",
        }
        assert set(inspection["resultado"].unique()).issubset(validos)

    def test_tipos_irregularidade_validos(self, inspection):
        validos = {
            "desvio_direto", "adulteracao_medidor", "ligacao_clandestina",
            "bypass", "adulteracao_lacre", None,
        }
        presentes = set(inspection["tipo_irregularidade"].unique())
        # pandas lê None como NaN; normaliza
        presentes_norm = {v if pd.notna(v) else None for v in presentes}
        assert presentes_norm.issubset(validos)

    # E1 — reincidente_flag deve ser True quando resultado é reincidencia_confirmada
    def test_reincidencia_confirmada_flag_verdadeiro(self, inspection):
        """Toda inspeção com resultado reincidencia_confirmada deve ter reincidente_flag=True."""
        violacoes = inspection[
            (inspection["resultado"] == "reincidencia_confirmada") &
            (~inspection["reincidente_flag"].astype(bool))
        ]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} reincidencias_confirmadas com reincidente_flag=False"
        )

    # E2 — reincidencia_confirmada requer inspeção anterior na mesma UC
    def test_reincidencia_nao_e_primeira_inspecao(self, inspection):
        """Uma UC não pode ter sua PRIMEIRA inspeção com resultado reincidencia_confirmada."""
        df = inspection[["consumer_id", "data_inspecao", "resultado"]].copy()
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])
        df = df.sort_values(["consumer_id", "data_inspecao"])
        primeiras = df.groupby("consumer_id").first().reset_index()
        violacoes = primeiras[primeiras["resultado"] == "reincidencia_confirmada"]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} UCs cuja PRIMEIRA inspeção já é reincidencia_confirmada"
        )

    # N3 — reincidente_flag determinístico: True para qualquer inspeção após confirmada
    def test_reincidente_flag_apos_qualquer_confirmacao(self, inspection):
        """Toda inspeção de uma UC que já teve irregularidade confirmada deve ter flag=True."""
        df = inspection[["consumer_id", "data_inspecao", "resultado", "reincidente_flag"]].copy()
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])
        df = df.sort_values(["consumer_id", "data_inspecao"])
        # Q1: só irregularidade_confirmada (reincidencia_confirmada não é mais um valor válido)
        confirmadas = {"irregularidade_confirmada"}
        violacoes = []
        for cid, grp in df.groupby("consumer_id"):
            ja_confirmou = False
            for _, row in grp.iterrows():
                if ja_confirmou and not row["reincidente_flag"]:
                    violacoes.append(cid)
                if row["resultado"] in confirmadas:
                    ja_confirmou = True
        assert len(violacoes) == 0, (
            f"{len(violacoes)} inspeções pós-confirmação com reincidente_flag=False: {violacoes[:5]}"
        )


# ─────────────────────────────────────────────
# meter_reader
# ─────────────────────────────────────────────

class TestMeterReader:

    # M2 — leituristas não devem ter tratamentos honoríficos
    def test_sem_honorificos_em_nomes(self, meter_reader):
        """Nomes de leituristas não devem ter prefixos honoríficos (Sr., Sra., Dr., Dra.)."""
        # usa regex sem grupos de captura para evitar UserWarning do pandas
        HONORIFIC_RE = r"(?:Sr\.|Sra\.|Dr\.|Dra\.|Prof\.|Profa\.)"
        violacoes = meter_reader[meter_reader["nome"].str.contains(HONORIFIC_RE, regex=True, na=False)]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} leituristas com honoríficos: "
            f"{violacoes['nome'].tolist()[:5]}"
        )


# ─────────────────────────────────────────────
# reading_occurrence — tipos válidos
# ─────────────────────────────────────────────

class TestReadingOccurrenceTypes:

    # M1 — 'media' foi renomeado para 'leitura_estimada'
    def test_sem_tipo_media_legado(self, reading_occurrence):
        """O tipo 'media' (legado) não deve aparecer — use 'leitura_estimada'."""
        com_media = (reading_occurrence["tipo_ocorrencia"] == "media").sum()
        assert com_media == 0, (
            f"{com_media} ocorrências ainda com tipo 'media' (deve ser 'leitura_estimada')"
        )

    def test_leitura_estimada_presente(self, reading_occurrence):
        """O tipo 'leitura_estimada' deve estar presente nas ocorrências."""
        presentes = set(reading_occurrence["tipo_ocorrencia"].unique())
        assert "leitura_estimada" in presentes, (
            f"Tipo 'leitura_estimada' não encontrado. Tipos: {presentes}"
        )


# ─────────────────────────────────────────────
# declared_load
# ─────────────────────────────────────────────

class TestDeclaredLoad:

    def test_potencia_positiva(self, declared_load):
        assert (declared_load["potencia_kw"] > 0).all()

    def test_horas_dia_intervalo(self, declared_load):
        fora = declared_load[
            (declared_load["horas_dia"] <= 0) |
            (declared_load["horas_dia"] > 24)
        ]
        assert len(fora) == 0

    # R2 — equipamentos de uso pontual não devem ultrapassar 3 horas/dia
    def test_horas_dia_por_equipamento(self, declared_load):
        """Chuveiro, micro-ondas e similares têm uso pontual (máx 2h/dia)."""
        uso_pontual = {"chuveiro_eletrico", "micro_ondas", "aquecedor_de_passagem", "maquina_lavar"}
        dl_pontual = declared_load[declared_load["equipamento"].isin(uso_pontual)]
        violacoes = dl_pontual[dl_pontual["horas_dia"] > 3.0]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} equipamentos de uso pontual com horas_dia > 3: "
            f"{violacoes[['equipamento','horas_dia']].drop_duplicates().head().to_dict('records')}"
        )

    def test_sem_equipamento_nulo(self, declared_load):
        assert declared_load["equipamento"].isna().sum() == 0

    # N5 — mínimo 2 equipamentos por UC
    def test_min_dois_equipamentos_por_uc(self, declared_load):
        """Toda UC deve declarar ao menos 2 equipamentos."""
        por_uc = declared_load.groupby("consumer_id").size()
        com_apenas_1 = por_uc[por_uc < 2]
        assert len(com_apenas_1) == 0, (
            f"{len(com_apenas_1)} UCs com menos de 2 equipamentos declarados"
        )

    # O6 — equipamentos industriais não devem aparecer em UCs residenciais
    def test_sem_equip_industrial_em_residencial(self, declared_load, consumer_unit):
        """forno_industrial, prensa_hidraulica e torno_mecanico só são válidos em industrial/comercial."""
        INDUSTRIAIS = {"forno_industrial", "prensa_hidraulica", "torno_mecanico"}
        res_ids = set(consumer_unit[consumer_unit["tipo_consumidor"] == "residencial"]["consumer_id"])
        dl_res = declared_load[declared_load["consumer_id"].isin(res_ids)]
        violacoes = dl_res[dl_res["equipamento"].isin(INDUSTRIAIS)]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} equipamentos industriais em UCs residenciais: "
            f"{violacoes[['consumer_id','equipamento']].head().to_dict('records')}"
        )

    # P4 — elevador_residencial não deve aparecer em UCs residenciais
    def test_sem_elevador_residencial_em_residencial(self, declared_load, consumer_unit):
        """elevador_residencial é implausível em casas/aptos simples brasileiros."""
        res_ids = set(consumer_unit[consumer_unit["tipo_consumidor"] == "residencial"]["consumer_id"])
        dl_res = declared_load[declared_load["consumer_id"].isin(res_ids)]
        violacoes = dl_res[dl_res["equipamento"] == "elevador_residencial"]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} ocorrências de elevador_residencial em UCs residenciais"
        )


# ─────────────────────────────────────────────
# economic_activity
# ─────────────────────────────────────────────

class TestEconomicActivity:

    def test_cnae_formato(self, economic_activity):
        """Código CNAE no formato NNNN-N/NN."""
        invalidos = economic_activity[
            ~economic_activity["cnae"].str.match(r"^\d{4}-\d/\d{2}$")
        ]
        assert len(invalidos) == 0, f"CNAEs fora do formato:\n{invalidos['cnae'].tolist()}"

    def test_consumo_medio_positivo(self, economic_activity):
        assert (economic_activity["consumo_medio_categoria_kwh"] > 0).all()

    def test_tres_grupos_representados(self, economic_activity):
        """Deve haver CNAEs dos grupos comercial, industrial e rural."""
        # Rural começa com 0; industrial inclui 21xx, 23xx; comercial 47xx, 56xx etc.
        codigos = economic_activity["cnae"].tolist()
        tem_rural      = any(c.startswith("0") for c in codigos)
        tem_industrial = any(c[:2] in ("23", "24", "15", "20", "29", "16", "21", "22", "13", "41") for c in codigos)
        tem_comercial  = any(c[:2] in ("47", "56", "55", "46", "49", "80", "86", "85", "71", "62") for c in codigos)
        assert tem_rural,      "Nenhum CNAE rural encontrado"
        assert tem_industrial, "Nenhum CNAE industrial encontrado"
        assert tem_comercial,  "Nenhum CNAE comercial encontrado"


# ─────────────────────────────────────────────
# external_property_data
# ─────────────────────────────────────────────

class TestExternalPropertyData:

    # N4 — área coerente com tipo de consumidor
    def test_residencial_area_maxima_280m2(self, external_property_data, consumer_unit):
        """Residencial nunca deve ter área construída acima de 280 m²."""
        res_ids = set(consumer_unit[consumer_unit["tipo_consumidor"] == "residencial"]["consumer_id"])
        res_epd = external_property_data[external_property_data["consumer_id"].isin(res_ids)]
        grandes = res_epd[res_epd["area_construida_m2"] > 280]
        assert len(grandes) == 0, (
            f"{len(grandes)} residenciais com área >280m²: "
            f"{grandes[['consumer_id','area_construida_m2']].head().to_dict('records')}"
        )

    def test_area_positiva(self, external_property_data):
        assert (external_property_data["area_construida_m2"] > 0).all()

    # R4 — toda UC deve ter dado externo de propriedade (cobertura 100%)
    def test_toda_uc_tem_epd(self, external_property_data, consumer_unit):
        """Todo consumer_id em consumer_unit deve aparecer em external_property_data."""
        ucs_com_epd = set(external_property_data["consumer_id"])
        ucs_sem_epd = set(consumer_unit["consumer_id"]) - ucs_com_epd
        assert len(ucs_sem_epd) == 0, (
            f"{len(ucs_sem_epd)} UCs sem external_property_data: {sorted(ucs_sem_epd)[:5]}"
        )


# ─────────────────────────────────────────────
# work_order
# ─────────────────────────────────────────────

class TestWorkOrder:

    # O4 — OS não devem ser atribuídas a eletricistas inativos
    def test_os_apenas_eletricistas_ativos(self, work_order, electrician):
        """Nenhuma OS pode ter eletricista com status='inativo'."""
        inativos = set(electrician.loc[electrician["status"] == "inativo", "eletricista_id"])
        violacoes = work_order[work_order["eletricista_id"].isin(inativos)]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} OSs atribuídas a eletricistas inativos: "
            f"{violacoes['eletricista_id'].unique().tolist()}"
        )

    # O5 — toda UC inspecionada deve ter ao menos uma OS registrada
    def test_uc_inspecionada_tem_work_order(self, work_order, inspection):
        """Uma UC inspecionada deve ter ao menos uma ordem de serviço associada."""
        ucs_com_wo  = set(work_order["consumer_id"].unique())
        ucs_inspect = set(inspection["consumer_id"].unique())
        sem_wo = ucs_inspect - ucs_com_wo
        assert len(sem_wo) == 0, (
            f"{len(sem_wo)} UCs inspecionadas sem nenhuma OS: {sorted(sem_wo)[:5]}"
        )

    # P3 — toda UC de fraude deve ter ao menos 1 OS do tipo vistoria_fraude
    def test_fraude_uc_tem_vistoria_fraude(self, work_order, inspection):
        """Todo UC que passou por inspeção de irregularidade deve ter pelo menos 1 OS vistoria_fraude."""
        # Q1: apenas irregularidade_confirmada (único resultado válido de fraud)
        ucs_fraud_insp = set(
            inspection[
                inspection["resultado"] == "irregularidade_confirmada"
            ]["consumer_id"].unique()
        )
        if len(ucs_fraud_insp) == 0:
            pytest.skip("Nenhuma UC com irregularidade confirmada no dataset")
        ucs_com_vistoria = set(
            work_order[work_order["tipo_servico"] == "vistoria_fraude"]["consumer_id"].unique()
        )
        sem_vistoria = ucs_fraud_insp - ucs_com_vistoria
        assert len(sem_vistoria) == 0, (
            f"{len(sem_vistoria)} UCs com irregularidade confirmada sem OS vistoria_fraude: "
            f"{sorted(sem_vistoria)[:5]}"
        )

    # Q3 — vistoria_fraude deve ocorrer ANTES da inspeção de fraude na mesma UC
    def test_vistoria_fraude_anterior_a_inspecao(self, work_order, inspection):
        """A OS de vistoria_fraude deve preceder temporalmente a inspeção da UC."""
        first_vistoria = (
            work_order[work_order["tipo_servico"] == "vistoria_fraude"]
            .groupby("consumer_id")["data_execucao"].min()
        )
        first_inspecao = (
            inspection
            .groupby("consumer_id")["data_inspecao"].min()
        )
        merged = pd.concat([first_vistoria.rename("wo"), first_inspecao.rename("ins")], axis=1).dropna()
        merged["wo"]  = pd.to_datetime(merged["wo"])
        merged["ins"] = pd.to_datetime(merged["ins"])
        violacoes = merged[merged["wo"] > merged["ins"]]
        assert len(violacoes) == 0, (
            f"{len(violacoes)} UCs com vistoria_fraude DEPOIS da inspeção: "
            f"{violacoes.index.tolist()[:5]}"
        )
