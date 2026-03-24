"""
static.py — Tabelas de referência: economic_activity e address.
Não dependem de nenhuma outra tabela.
"""

from __future__ import annotations

import pandas as pd
from generators.base import BaseGenerator

# ---------------------------------------------------------------------------
# Pools de CNAE separados por tipo de consumidor
# Fonte: CNAE 2.3 (IBGE). Consumo médio mensal em kWh estimado por categoria.
# ---------------------------------------------------------------------------

# Comercial — seções G, H, I, J, K, L, M, N, P, Q, S
CNAES_COMERCIAL = [
    ("4711-3/01", "Comércio varejista de mercadorias em geral",                1800),
    ("4712-1/00", "Comércio varejista de alimentos em geral",                  1400),
    ("5611-2/01", "Restaurantes e similares",                                  2500),
    ("4781-4/00", "Comércio varejista de vestuário",                            900),
    ("4721-1/02", "Padaria e confeitaria com predominância de produção própria", 1200),
    ("4530-7/03", "Comércio varejista de peças e acessórios para veículos",    1500),
    ("4771-7/01", "Comércio varejista de produtos farmacêuticos",              1000),
    ("4744-0/05", "Comércio varejista de materiais de construção",             1700),
    ("8011-1/01", "Atividades de vigilância",                                  3200),
    ("6201-5/00", "Desenvolvimento de programas de computador sob encomenda",  1200),
    ("7319-0/04", "Agências de publicidade",                                   1000),
    ("8599-6/04", "Treinamento em desenvolvimento profissional",                700),
    ("8630-5/04", "Atividades de fisioterapia",                                 800),
    ("5510-8/01", "Hotéis",                                                    4200),
    ("4930-2/01", "Transporte rodoviário de carga em geral",                   3600),
    ("4110-7/00", "Incorporação de empreendimentos imobiliários",              2000),
    ("8621-6/01", "Serviços de remoção de pacientes — UTI móvel",              2200),
    ("4921-3/01", "Transporte rodoviário coletivo de passageiros, municipal",  2800),
    ("4542-1/06", "Comércio varejista de motocicletas e motonetas",            1300),
    ("8711-5/01", "Clínicas e residências geriátricas",                        3000),
]

# Industrial — seções B, C, D, F
CNAES_INDUSTRIAL = [
    ("2311-7/00", "Fabricação de vidro plano",                                 8000),
    ("1091-1/02", "Fabricação de produtos de panificação industrial",          4500),
    ("2330-3/01", "Fabricação de estruturas pré-moldadas de concreto",         7000),
    ("4211-1/01", "Construção de rodovias e ferrovias (matriz)",              12000),
    ("3520-4/01", "Produção de gás; processamento de gás natural",            18000),
    ("2421-1/00", "Produção de ferro-gusa",                                   22000),
    ("1531-9/01", "Fabricação de calçados de couro",                           5500),
    ("2013-4/01", "Fabricação de adubos e fertilizantes",                     15000),
    ("2940-0/00", "Fabricação de peças e acessórios para veículos",            9000),
    ("1412-6/01", "Confecção de peças do vestuário, exceto roupas íntimas",   3800),
    ("1610-2/03", "Serrarias com desdobramento de madeira em bruto",           6000),
    ("2219-6/00", "Fabricação de artefatos de borracha não especificados",     7500),
]

# Rural — seção A (agropecuária, pesca e aquicultura)
CNAES_RURAL = [
    ("0111-3/01", "Cultivo de arroz",                                           400),
    ("0111-3/03", "Cultivo de milho",                                           350),
    ("0115-6/00", "Cultivo de soja",                                            420),
    ("0151-2/01", "Criação de bovinos para corte",                              800),
    ("0151-2/02", "Criação de bovinos para leite",                              900),
    ("0155-5/01", "Criação de frangos para corte",                             1200),
    ("0159-8/02", "Criação de suínos",                                          750),
    ("0131-8/00", "Horticultura",                                               300),
    ("0133-4/99", "Cultivo de frutas de lavoura permanente",                    280),
    ("0161-0/01", "Serviço de pulverização e controle de pragas agrícolas",     500),
    ("0210-1/01", "Cultivo de eucalipto",                                       600),
]

# Tabela completa (usada pela economic_activity)
ALL_CNAES = CNAES_COMERCIAL + CNAES_INDUSTRIAL + CNAES_RURAL

# Apenas códigos por pool (importados pelo consumer_unit.py)
CNAE_CODES_COMERCIAL  = [c[0] for c in CNAES_COMERCIAL]
CNAE_CODES_INDUSTRIAL = [c[0] for c in CNAES_INDUSTRIAL]
CNAE_CODES_RURAL      = [c[0] for c in CNAES_RURAL]

# ---------------------------------------------------------------------------
# Endereços reais — área de concessão no interior paulista
# Referência: municípios típicos de distribuidoras como CPFL Paulista / CPFL Santa Cruz
# Renda média mensal domiciliar em R$ (base IBGE 2022, ajustada para 2023)
# ---------------------------------------------------------------------------

ADDRESSES = [
    # Campinas (SP)
    ("Centro",               "Campinas",              "SP", 4200),
    ("Cambuí",               "Campinas",              "SP", 7800),
    ("Jardim Proença",       "Campinas",              "SP", 2800),
    ("DIC I",                "Campinas",              "SP", 1750),
    ("Parque Itália",        "Campinas",              "SP", 2100),
    # Piracicaba (SP)
    ("Centro",               "Piracicaba",            "SP", 3800),
    ("Jardim Oriente",       "Piracicaba",            "SP", 2400),
    ("São Dimas",            "Piracicaba",            "SP", 3200),
    ("Nova América",         "Piracicaba",            "SP", 1900),
    # Limeira (SP)
    ("Centro",               "Limeira",               "SP", 3400),
    ("Jardim Santa Rosa",    "Limeira",               "SP", 2100),
    ("Parque Industrial",    "Limeira",               "SP", 2600),
    # Americana (SP)
    ("Centro",               "Americana",             "SP", 3600),
    ("Nova Americana",       "Americana",             "SP", 2200),
    ("Jardim Brasil",        "Americana",             "SP", 2900),
    # Sumaré (SP)
    ("Centro",               "Sumaré",                "SP", 2900),
    ("Jardim Tulipas",       "Sumaré",                "SP", 2100),
    ("Distrito Industrial",  "Sumaré",                "SP", 2700),
    # Santa Bárbara d'Oeste (SP)
    ("Centro",               "Santa Bárbara d'Oeste", "SP", 3100),
    ("Jardim Pérola",        "Santa Bárbara d'Oeste", "SP", 2300),
]


class EconomicActivityGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        df = pd.DataFrame(ALL_CNAES, columns=["cnae", "descricao", "consumo_medio_categoria_kwh"])
        self.save(df, "economic_activity")
        return df


class AddressGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        rows = []
        for idx, (bairro, cidade, estado, renda) in enumerate(ADDRESSES, start=1):
            rows.append({
                "endereco_id": f"ADDR-{idx:04d}",
                "bairro": bairro,
                "cidade": cidade,
                "estado": estado,
                "renda_media_regiao": renda,
            })
        df = pd.DataFrame(rows)
        self.save(df, "address")
        return df
