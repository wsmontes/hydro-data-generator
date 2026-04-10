"""
reference.py — Tabelas de referência IBGE: geo_municipio e cnae.
"""

from __future__ import annotations

import pandas as pd

from generators.base import BaseGenerator
from generators.static import ALL_CNAES


# ---------------------------------------------------------------------------
# Municípios da área de concessão com códigos IBGE reais
# ---------------------------------------------------------------------------

MUNICIPIOS = [
    (3509502, "Campinas",              "SP", "SUDESTE", "Campinas",    "Campinas"),
    (3538709, "Piracicaba",            "SP", "SUDESTE", "Piracicaba",  "Piracicaba"),
    (3526902, "Limeira",               "SP", "SUDESTE", "Campinas",    "Limeira"),
    (3501608, "Americana",             "SP", "SUDESTE", "Campinas",    "Americana"),
    (3552403, "Sumaré",                "SP", "SUDESTE", "Campinas",    "Campinas"),
    (3545803, "Santa Bárbara d'Oeste", "SP", "SUDESTE", "Piracicaba",  "Piracicaba"),
]


# ---------------------------------------------------------------------------
# CNAE: mapeamento de divisão → seção IBGE (CNAE 2.3)
# ---------------------------------------------------------------------------

_DIVISAO_TO_SECAO = {
    "01": "A", "02": "A", "03": "A",
    "05": "B", "06": "B", "07": "B", "08": "B", "09": "B",
    "10": "C", "11": "C", "12": "C", "13": "C", "14": "C", "15": "C",
    "16": "C", "17": "C", "18": "C", "19": "C", "20": "C", "21": "C",
    "22": "C", "23": "C", "24": "C", "25": "C", "26": "C", "27": "C",
    "28": "C", "29": "C", "30": "C", "31": "C", "32": "C", "33": "C",
    "35": "D",
    "36": "E", "37": "E", "38": "E", "39": "E",
    "41": "F", "42": "F", "43": "F",
    "45": "G", "46": "G", "47": "G",
    "49": "H", "50": "H", "51": "H", "52": "H", "53": "H",
    "55": "I", "56": "I",
    "58": "J", "59": "J", "60": "J", "61": "J", "62": "J", "63": "J",
    "64": "K", "65": "K", "66": "K",
    "68": "L",
    "69": "M", "70": "M", "71": "M", "72": "M", "73": "M", "74": "M", "75": "M",
    "77": "N", "78": "N", "79": "N", "80": "N", "81": "N", "82": "N",
    "84": "O",
    "85": "P",
    "86": "Q", "87": "Q", "88": "Q",
    "90": "R", "91": "R", "92": "R", "93": "R",
    "94": "S", "95": "S", "96": "S",
    "97": "T",
    "99": "U",
}


def _parse_cnae(cnae_code: str) -> dict:
    """Extrai grupo, divisão e seção de um código CNAE formatado."""
    raw = cnae_code.replace("-", "").replace("/", "")
    divisao = raw[:2]
    grupo = raw[:3]
    secao = _DIVISAO_TO_SECAO.get(divisao, "?")
    return {"divisao": divisao, "grupo": grupo, "secao": secao}


class GeoMunicipioGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        rows = []
        for mid, nome, uf, regiao, meso, micro in MUNICIPIOS:
            rows.append({
                "municipio_id": mid,
                "nome": nome,
                "uf": uf,
                "regiao": regiao,
                "mesorregiao": meso,
                "microrregiao": micro,
            })
        df = pd.DataFrame(rows)
        self.save(df, "geo_municipio")
        return df


class CnaeReferenceGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        rows = []
        seen = set()
        for cnae_code, descricao, _ in ALL_CNAES:
            if cnae_code in seen:
                continue
            seen.add(cnae_code)
            parsed = _parse_cnae(cnae_code)
            raw = cnae_code.replace("-", "").replace("/", "")
            rows.append({
                "cnae": raw,
                "descricao": descricao,
                "grupo": parsed["grupo"],
                "divisao": parsed["divisao"],
                "secao": parsed["secao"],
            })
        df = pd.DataFrame(rows)
        self.save(df, "cnae")
        return df
