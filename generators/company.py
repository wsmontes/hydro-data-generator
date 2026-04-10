"""
company.py — Dados da Receita Federal: company, company_cnae, company_qsa.

Vínculos com dados existentes:
- 100% dos clientes PJ existentes → registro de empresa
- +15% empresas extras (fornecedores, parceiras na região)
- CNAE primária da empresa = CNAE da consumer_unit vinculada
- ~30% dos CPFs PF aparecem como sócios (QSA)
"""

from __future__ import annotations

import random

import pandas as pd
from faker import Faker

from generators.base import BaseGenerator
from generators.static import CNAE_CODES_COMERCIAL, CNAE_CODES_INDUSTRIAL

fake_br = Faker("pt_BR")
fake_br.seed_instance(42)

SITUACOES_CADASTRAIS = ["ATIVA", "BAIXADA", "SUSPENSA", "INAPTA"]
PORTES = ["ME", "EPP", "DEMAIS"]
NATUREZAS_JURIDICAS = [
    "2062",  # Soc. Empresária Limitada
    "2046",  # Soc. Anônima Fechada
    "2135",  # Empresário Individual
    "2305",  # EIRELI
    "2313",  # Soc. Simples
    "2143",  # Cooperativa
]

QUALIFICACOES = [
    "49-Sócio-Administrador",
    "22-Sócio",
    "10-Diretor",
    "05-Administrador",
    "16-Presidente",
    "54-Quotista Minoritário",
]

_SUFIXOS_PJ = (" Ltda.", " S.A.", " S/A", " - ME", " - EI", " e Filhos", " & Cia.")
_SUFIXOS_CHECK = ("ltda", "s.a", "s/a", "filhos", "- me", "- ei")


def _razao_social() -> str:
    nome = fake_br.company()
    if not any(s in nome.lower() for s in _SUFIXOS_CHECK):
        nome += random.choice(_SUFIXOS_PJ)
    return nome


def _raw_cnae(code: str) -> str:
    """'4711-3/01' → '4711301'"""
    return code.replace("-", "").replace("/", "")


def _random_percentages(n: int) -> list[float]:
    """Porcentagens de participação que somam 100%."""
    if n == 1:
        return [100.0]
    weights = [random.uniform(1, 5) for _ in range(n)]
    total = sum(weights)
    parts = [round(w / total * 100, 2) for w in weights]
    parts[-1] = round(100.0 - sum(parts[:-1]), 2)
    return parts


class CompanyGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        ext = self.config.get("external", {})
        extra_pct = ext.get("companies_extra_pct", 0.15)
        qsa_pf_pct = ext.get("qsa_pf_participation_pct", 0.30)

        customer_df = self.ctx["customer"]
        cu_df = self.ctx.get("consumer_unit_full", self.ctx.get("consumer_unit"))
        address_df = self.ctx["address"]

        pj_customers = customer_df[customer_df["tipo"] == "PJ"]
        pf_customers = customer_df[customer_df["tipo"] == "PF"]

        # CNAE primária por cpf_cnpj (da consumer_unit)
        cu_cnae = (
            cu_df[cu_df["cnae"].notna()]
            .drop_duplicates("cpf_cnpj")
            .set_index("cpf_cnpj")["cnae"]
            .to_dict()
        )

        # Endereço por cpf_cnpj (via consumer_unit → address)
        cu_addr = cu_df[["cpf_cnpj", "endereco_id"]].drop_duplicates("cpf_cnpj")
        addr_merge = cu_addr.merge(address_df, on="endereco_id", how="left")
        addr_map = addr_merge.set_index("cpf_cnpj").to_dict("index")

        municipios = list(address_df["cidade"].unique())
        bairros_by_city = (
            address_df.groupby("cidade")["bairro"].apply(list).to_dict()
        )

        company_rows = []
        cnae_rows = []

        # ------------------------------------------------------------------
        # Empresas de clientes PJ existentes
        # ------------------------------------------------------------------
        for _, pj in pj_customers.iterrows():
            cnpj_fmt = pj["cpf_cnpj"]

            addr = addr_map.get(pj["cpf_cnpj"], {})
            cidade = addr.get("cidade", random.choice(municipios))
            bairro = addr.get(
                "bairro", random.choice(bairros_by_city.get(cidade, ["Centro"]))
            )

            data_abertura = self.random_date(
                start=pd.Timestamp("2000-01-01"),
                end=self.date_start,
            )
            porte = random.choices(PORTES, weights=[50, 35, 15], k=1)[0]
            capital = round(random.uniform(1_000, 5_000_000), 2)
            if porte == "DEMAIS":
                capital = round(random.uniform(500_000, 50_000_000), 2)

            company_rows.append({
                "cnpj": cnpj_fmt,
                "razao_social": pj["nome"],
                "nome_fantasia": fake_br.company() if random.random() < 0.7 else "",
                "situacao_cadastral": random.choices(
                    SITUACOES_CADASTRAIS, weights=[85, 5, 5, 5], k=1
                )[0],
                "data_situacao": self.random_date(
                    start=data_abertura, end=self.date_end
                ).strftime("%Y-%m-%d"),
                "data_abertura": data_abertura.strftime("%Y-%m-%d"),
                "natureza_juridica": random.choice(NATUREZAS_JURIDICAS),
                "porte": porte,
                "capital_social": capital,
                "simples_nacional": random.random() < 0.45,
                "mei": porte == "ME" and random.random() < 0.30,
                "email": fake_br.company_email(),
                "telefone": fake_br.phone_number(),
                "logradouro": fake_br.street_name(),
                "numero": str(random.randint(1, 9999)),
                "complemento": random.choice(
                    ["Sala 1", "Sala 2", "Galpão A", "Bloco B", "", "", ""]
                ),
                "bairro": bairro,
                "municipio": cidade,
                "uf": "SP",
                "cep": fake_br.postcode().replace("-", ""),
            })

            # CNAE primária (da consumer_unit se disponível)
            primary = cu_cnae.get(pj["cpf_cnpj"])
            if primary:
                cnae_rows.append({
                    "cnpj": cnpj_fmt,
                    "cnae": _raw_cnae(primary),
                    "tipo": "PRINCIPAL",
                })
            else:
                fallback = random.choice(CNAE_CODES_COMERCIAL)
                cnae_rows.append({
                    "cnpj": cnpj_fmt,
                    "cnae": _raw_cnae(fallback),
                    "tipo": "PRINCIPAL",
                })

            # Secundárias (0-3)
            all_codes = CNAE_CODES_COMERCIAL + CNAE_CODES_INDUSTRIAL
            n_sec = random.randint(0, 3)
            for sec in random.sample(all_codes, k=min(n_sec, len(all_codes))):
                cnae_rows.append({
                    "cnpj": cnpj_fmt,
                    "cnae": _raw_cnae(sec),
                    "tipo": "SECUNDARIO",
                })

        # ------------------------------------------------------------------
        # Empresas extras (fornecedores, parceiras na região)
        # ------------------------------------------------------------------
        n_extra = int(len(pj_customers) * extra_pct)
        for _ in range(n_extra):
            cnpj_fmt = fake_br.cnpj()
            cidade = random.choice(municipios)
            bairro = random.choice(bairros_by_city.get(cidade, ["Centro"]))
            data_abertura = self.random_date(
                start=pd.Timestamp("2000-01-01"), end=self.date_end
            )
            porte = random.choices(PORTES, weights=[40, 35, 25], k=1)[0]
            capital = round(random.uniform(5_000, 2_000_000), 2)

            company_rows.append({
                "cnpj": cnpj_fmt,
                "razao_social": _razao_social(),
                "nome_fantasia": fake_br.company() if random.random() < 0.6 else "",
                "situacao_cadastral": random.choices(
                    SITUACOES_CADASTRAIS, weights=[80, 8, 6, 6], k=1
                )[0],
                "data_situacao": self.random_date(
                    start=data_abertura, end=self.date_end
                ).strftime("%Y-%m-%d"),
                "data_abertura": data_abertura.strftime("%Y-%m-%d"),
                "natureza_juridica": random.choice(NATUREZAS_JURIDICAS),
                "porte": porte,
                "capital_social": capital,
                "simples_nacional": random.random() < 0.50,
                "mei": porte == "ME" and random.random() < 0.35,
                "email": fake_br.company_email(),
                "telefone": fake_br.phone_number(),
                "logradouro": fake_br.street_name(),
                "numero": str(random.randint(1, 9999)),
                "complemento": random.choice(["", "", "", "Sala 1", "Galpão"]),
                "bairro": bairro,
                "municipio": cidade,
                "uf": "SP",
                "cep": fake_br.postcode().replace("-", ""),
            })

            cnae_code = random.choice(CNAE_CODES_COMERCIAL + CNAE_CODES_INDUSTRIAL)
            cnae_rows.append({
                "cnpj": cnpj_fmt,
                "cnae": _raw_cnae(cnae_code),
                "tipo": "PRINCIPAL",
            })

        company_df = pd.DataFrame(company_rows)
        company_cnae_df = pd.DataFrame(cnae_rows)

        # ------------------------------------------------------------------
        # QSA — Quadro Societário e de Administradores
        # ------------------------------------------------------------------
        qsa_rows = []
        pf_cpfs = list(pf_customers["cpf_cnpj"])
        pf_names = dict(zip(pf_customers["cpf_cnpj"], pf_customers["nome"]))

        n_pf_socios = int(len(pf_cpfs) * qsa_pf_pct)
        socios_pool = random.sample(pf_cpfs, k=min(n_pf_socios, len(pf_cpfs)))

        socio_idx = 0
        for _, comp in company_df.iterrows():
            cnpj = comp["cnpj"]
            n_socios = random.choices([1, 2, 3, 4], weights=[20, 45, 25, 10], k=1)[0]
            percents = _random_percentages(n_socios)

            for j in range(n_socios):
                if socio_idx < len(socios_pool):
                    cpf_val = socios_pool[socio_idx]
                    nome = pf_names.get(cpf_val, fake_br.name())
                    socio_idx += 1
                else:
                    cpf_val = fake_br.cpf()
                    nome = fake_br.name()

                qsa_rows.append({
                    "cnpj": cnpj,
                    "cpf_socio": cpf_val,
                    "nome_socio": nome,
                    "qualificacao": random.choice(QUALIFICACOES),
                    "percentual_participacao": percents[j],
                })

        qsa_df = pd.DataFrame(qsa_rows)

        # Salva tudo
        self.save(company_df, "company")
        self.save(company_cnae_df, "company_cnae")
        self.save(qsa_df, "company_qsa")

        self.ctx["company"] = company_df
        self.ctx["company_cnae"] = company_cnae_df
        self.ctx["company_qsa"] = qsa_df

        return company_df
