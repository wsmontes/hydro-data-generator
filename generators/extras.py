"""
extras.py — Geradores de: declared_load e external_property_data.
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator

# O6: catálogos separados por tipo de consumidor — sem equip. industriais em residenciais
_EQUIP_RESIDENCIAL = [
    ("ar_condicionado_split_9k",   0.9),
    ("ar_condicionado_split_12k",  1.2),
    ("ar_condicionado_split_18k",  1.8),
    ("chuveiro_eletrico",          5.5),
    ("aquecedor_de_passagem",      7.5),
    ("geladeira_frost_free",       0.15),
    ("freezer_horizontal",         0.25),
    ("televisao_55",               0.12),
    ("computador_desktop",         0.30),
    ("bomba_dagua_1cv",            0.75),
    ("maquina_lavar",              0.60),   # P4: substitui elevador_residencial (irreal)
    ("micro_ondas",                1.50),
]
_EQUIP_COMERCIAL = [
    ("ar_condicionado_split_12k",  1.2),
    ("ar_condicionado_split_18k",  1.8),
    ("computador_desktop",         0.30),
    ("bomba_dagua_1cv",            0.75),
    ("forno_industrial",          12.0),
    ("elevador_comercial",         5.0),   # P4: elevador pláusivel em edificio comercial
    ("televisao_55",               0.12),
    ("freezer_horizontal",         0.25),
]
_EQUIP_INDUSTRIAL = [
    ("forno_industrial",          12.0),
    ("prensa_hidraulica",         22.0),
    ("torno_mecanico",            15.0),
    ("bomba_dagua_1cv",            0.75),
    ("ar_condicionado_split_18k",  1.8),
    ("computador_desktop",         0.30),
]
_EQUIP_RURAL = [
    ("bomba_dagua_1cv",            0.75),
    ("sistema_irrigacao",          5.0),
    ("geladeira_frost_free",       0.15),
    ("televisao_55",               0.12),
    ("computador_desktop",         0.30),
    ("ar_condicionado_split_9k",   0.9),
    ("aquecedor_de_passagem",      7.5),
]
_EQUIP_BY_TYPE = {
    "residencial": _EQUIP_RESIDENCIAL,
    "comercial":   _EQUIP_COMERCIAL,
    "industrial":  _EQUIP_INDUSTRIAL,
    "rural":       _EQUIP_RURAL,
}
# backward-compat alias (testes antigos referenciam EQUIPAMENTOS)
EQUIPAMENTOS = _EQUIP_RESIDENCIAL + _EQUIP_COMERCIAL + _EQUIP_INDUSTRIAL + _EQUIP_RURAL

FONTES_IMAGEM = ["Google Maps", "Bing Maps", "IBGE Imagem", "SRTM"]

# R2: faixas realistas de horas de uso diário por equipamento
# Valores baseados em padrões típicos de consumo residencial/comercial/industrial
_HORAS_DIA: dict[str, tuple[float, float]] = {
    # Climatização — uso intenso em horários de pico
    "ar_condicionado_split_9k":   (3.0, 12.0),
    "ar_condicionado_split_12k":  (3.0, 12.0),
    "ar_condicionado_split_18k":  (3.0, 12.0),
    # Aquecimento de água — uso curto e pontual
    "chuveiro_eletrico":          (0.3,  1.5),
    "aquecedor_de_passagem":      (0.3,  1.5),
    # Geração de frio — funcionamento quase contínuo
    "geladeira_frost_free":       (8.0, 18.0),
    "freezer_horizontal":         (8.0, 18.0),
    # Entretenimento / escritório
    "televisao_55":               (2.0,  8.0),
    "computador_desktop":         (4.0, 10.0),
    # Bomba e irrigação
    "bomba_dagua_1cv":            (1.0,  6.0),
    "sistema_irrigacao":          (1.0,  6.0),
    # Culinária / lavanderia
    "maquina_lavar":              (0.5,  2.0),
    "micro_ondas":                (0.3,  1.5),
    # Equipamentos comerciais / industriais
    "forno_industrial":           (4.0, 14.0),
    "elevador_comercial":         (4.0, 12.0),
    "prensa_hidraulica":          (4.0, 12.0),
    "torno_mecanico":             (4.0, 12.0),
}


# N5: mínimo de equipamentos por perfil de consumo
_MIN_EQUIP = {
    "residencial": 2,
    "comercial":   3,
    "industrial":  4,
    "rural":       2,
}
_MAX_EQUIP = {
    "residencial": 5,
    "comercial":   8,
    "industrial":  12,
    "rural":       5,
}


class DeclaredLoadGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        cu_df = self.ctx["consumer_unit_full"]

        rows = []
        for _, uc in cu_df.iterrows():
            tipo = uc["tipo_consumidor"]
            # N5: mínimo 2 equipamentos; comercial/industrial têm faixas maiores
            n_min   = _MIN_EQUIP.get(tipo, 2)
            n_max   = _MAX_EQUIP.get(tipo, 5)
            n_equip = random.randint(n_min, n_max)
            catalog = _EQUIP_BY_TYPE.get(tipo, _EQUIP_RESIDENCIAL)
            equips = random.sample(catalog, k=min(n_equip, len(catalog)))
            for equip_nome, potencia in equips:
                # R2: horas_dia realistas por tipo de equipamento
                h_min, h_max = _HORAS_DIA.get(equip_nome, (1.0, 12.0))
                rows.append({
                    "consumer_id": uc["consumer_id"],
                    "equipamento": equip_nome,
                    "potencia_kw": potencia,
                    "horas_dia": round(random.uniform(h_min, h_max), 1),
                    "data_declaracao": self.random_date().strftime("%Y-%m-%d"),
                })

        df = pd.DataFrame(rows)
        self.save(df, "declared_load")
        return df


# N4: faixas de área por tipo de consumidor
_AREA_RANGE = {
    "residencial": (30,    280),
    "comercial":   (50,    800),
    "industrial":  (200, 5000),
    "rural":       (100,  500),
}


class ExternalPropertyDataGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        cu_df = self.ctx["consumer_unit_full"]

        rows = []
        for _, uc in cu_df.iterrows():
            # R4: cobertura 100% — toda UC tem dado externo de propriedade
            tipo = uc["tipo_consumidor"]
            a_min, a_max = _AREA_RANGE.get(tipo, (30, 800))
            rows.append({
                "consumer_id": uc["consumer_id"],
                "area_construida_m2": round(random.uniform(a_min, a_max), 1),
                "data_imagem": self.random_date().strftime("%Y-%m-%d"),
                "fonte": random.choice(FONTES_IMAGEM),
            })

        df = pd.DataFrame(rows)
        self.save(df, "external_property_data")
        return df
