"""
financial.py — Dívida / Financeiro: financial_debt.

Vínculos:
- ~12% dos CPF/CNPJ possuem dívidas
- 60% dos fraudulentos têm dívida TRIBUTARIA ativa
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator


TIPOS_DIVIDA = ["TRIBUTARIA", "PREVIDENCIARIA", "MULTA", "OUTROS"]
STATUS_DIVIDA = ["ATIVA", "PARCELADA", "QUITADA"]


class FinancialDebtGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        ext = self.config.get("external", {})
        debt_rate = ext.get("debt_rate", 0.12)
        fraud_corr = ext.get("debt_fraud_correlation", 0.60)

        customer_df = self.ctx["customer"]
        fraud_set = self.ctx.get("_fraud_cpf_cnpj", set())

        fraud_customers = customer_df[customer_df["cpf_cnpj"].isin(fraud_set)]
        normal_customers = customer_df[~customer_df["cpf_cnpj"].isin(fraud_set)]

        n_fraud_debt = int(len(fraud_customers) * fraud_corr)
        n_normal_debt = int(len(normal_customers) * debt_rate)

        fraud_sample = (
            fraud_customers.sample(
                n=min(n_fraud_debt, len(fraud_customers)),
                random_state=self.seed,
            )
            if len(fraud_customers) > 0
            else pd.DataFrame()
        )
        normal_sample = (
            normal_customers.sample(
                n=min(n_normal_debt, len(normal_customers)),
                random_state=self.seed,
            )
            if len(normal_customers) > 0
            else pd.DataFrame()
        )

        debtors = pd.concat([fraud_sample, normal_sample], ignore_index=True)

        rows = []
        for _, debtor in debtors.iterrows():
            cpf_cnpj = debtor["cpf_cnpj"]
            is_fraud = cpf_cnpj in fraud_set

            n_debts = random.randint(1, 3) if is_fraud else random.randint(1, 2)

            for _ in range(n_debts):
                if is_fraud:
                    tipo = random.choices(
                        TIPOS_DIVIDA, weights=[50, 20, 20, 10], k=1
                    )[0]
                    status = random.choices(
                        STATUS_DIVIDA, weights=[60, 25, 15], k=1
                    )[0]
                    valor = round(random.uniform(2_000, 150_000), 2)
                else:
                    tipo = random.choices(
                        TIPOS_DIVIDA, weights=[30, 25, 25, 20], k=1
                    )[0]
                    status = random.choices(
                        STATUS_DIVIDA, weights=[30, 30, 40], k=1
                    )[0]
                    valor = round(random.uniform(500, 50_000), 2)

                rows.append({
                    "cpf_cnpj": cpf_cnpj,
                    "tipo_divida": tipo,
                    "valor": valor,
                    "status": status,
                    "data_inscricao": self.random_date(
                        start=self.date_start - pd.DateOffset(years=3),
                        end=self.date_end,
                    ).strftime("%Y-%m-%d"),
                })

        df = pd.DataFrame(rows)
        self.save(df, "financial_debt")
        self.ctx["financial_debt"] = df
        return df
