"""
people.py — Geradores de: customer, electrician, meter_reader.
"""

from __future__ import annotations

import random

import pandas as pd
from faker import Faker

from generators.base import BaseGenerator

fake_br = Faker("pt_BR")
fake_br.seed_instance(42)


# ---------------------------------------------------------------------------
# Customer
# ---------------------------------------------------------------------------

class CustomerGenerator(BaseGenerator):
    """
    Gera a tabela `customer` (pessoa física ou jurídica).
    CPF e CNPJ gerados com dígitos verificadores válidos via Faker pt_BR.
    Garante que alguns CPF/CNPJ aparecem em múltiplas consumer_units.
    """
    # N1: nomes de clientes PF refletem cadastro da Receita — sem honoríficos
    _HONORIFICOS = ("Sr. ", "Sra. ", "Dr. ", "Dra. ", "Prof. ", "Profa. ", "Srta. ", "Mr. ")
    # Q4: sufixos legais obrigatórios para razão social de PJ
    _SUFIXOS_PJ = (" Ltda.", " S.A.", " S/A", " - ME", " - EI", " e Filhos", " & Cia.")
    # "cia" removido: é substring de "garcia" → falso positivo em escala
    _SUFIXOS_PJ_LOWER = tuple(s.lower() for s in _SUFIXOS_PJ) + ("ltda", "s.a", "s/a", "filhos")

    def _nome_pf(self) -> str:
        nome = fake_br.name()
        for h in self._HONORIFICOS:
            if nome.startswith(h):
                nome = nome[len(h):]
                break
        return nome

    def _nome_pj(self) -> str:
        """Gera razão social de empresa, garantindo sufixo legal (Ltda., S.A., etc.)."""
        nome = fake_br.company()
        nome_lower = nome.lower()
        has_suffix = any(s in nome_lower for s in self._SUFIXOS_PJ_LOWER)
        if not has_suffix:
            nome = nome + random.choice(self._SUFIXOS_PJ)
        return nome

    def generate(self) -> pd.DataFrame:
        n_cu = self.config["volumes"]["consumer_units"]
        # ~85% PF (CPF), ~15% PJ (CNPJ)
        n_pf = int(n_cu * 0.85)
        n_pj = int(n_cu * 0.15) + 1

        rows = []
        for _ in range(n_pf):
            rows.append({
                "cpf_cnpj": fake_br.cpf(),
                "nome": self._nome_pf(),
                "tipo": "PF",
            })
        for _ in range(n_pj):
            rows.append({
                "cpf_cnpj": fake_br.cnpj(),
                "nome": self._nome_pj(),
                "tipo": "PJ",
            })

        df = pd.DataFrame(rows).drop_duplicates("cpf_cnpj").reset_index(drop=True)
        self.save(df, "customer")
        return df


# ---------------------------------------------------------------------------
# Electrician
# ---------------------------------------------------------------------------

EMPRESAS_ELETRICA = [
    "Voltec Serviços Elétricos",
    "Eletro Sul Manutenção",
    "PowerFix Instalações",
    "AmpereService Ltda",
    "JR Elétrica e Manutenção",
]

STATUS_ELETRICISTA = ["ativo", "ativo", "ativo", "suspenso", "inativo"]


class ElectricianGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        n = self.config["volumes"]["electricians"]
        rows = []
        for i in range(1, n + 1):
            rows.append({
                "eletricista_id": f"ELET-{i:04d}",
                "nome": fake_br.name(),
                "empresa": random.choice(EMPRESAS_ELETRICA),
                "status": random.choice(STATUS_ELETRICISTA),
            })
        df = pd.DataFrame(rows)
        self.save(df, "electrician")
        return df


# ---------------------------------------------------------------------------
# Meter Reader
# ---------------------------------------------------------------------------

class MeterReaderGenerator(BaseGenerator):
    # M2: leituristas são agentes de campo — sem tratamentos honoríficos
    _HONORIFICOS = ("Sr. ", "Sra. ", "Dr. ", "Dra. ", "Prof. ", "Profa. ")

    def _nome_campo(self) -> str:
        nome = fake_br.name()
        for h in self._HONORIFICOS:
            if nome.startswith(h):
                nome = nome[len(h):]
                break
        return nome

    def generate(self) -> pd.DataFrame:
        n = self.config["volumes"]["meter_readers"]
        rows = []
        for i in range(1, n + 1):
            rows.append({
                "reader_id": f"READ-{i:04d}",
                "nome": self._nome_campo(),
            })
        df = pd.DataFrame(rows)
        self.save(df, "meter_reader")
        return df
