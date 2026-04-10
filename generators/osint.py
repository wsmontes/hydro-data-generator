"""
osint.py — OSINT Events: osint_events.

Vínculos:
- ~4% dos CPF/CNPJ possuem eventos OSINT
- 70% das entidades sancionadas têm evento NEGATIVO em DIARIO_OFICIAL
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator

FONTES = ["NEWS", "BLOG", "SOCIAL", "DIARIO_OFICIAL"]

TITULOS_NEGATIVO = [
    "Empresa investigada por irregularidades fiscais",
    "Indivíduo citado em operação policial",
    "Sanção aplicada por órgão regulador",
    "Denúncia de fraude em setor energético",
    "Processo judicial por dívida tributária",
    "Investigação de desvio de recursos",
    "Auto de infração lavrado contra contribuinte",
    "Interdição de estabelecimento por irregularidade",
]

TITULOS_NEUTRO = [
    "Menção em publicação do Diário Oficial",
    "Registro de atividade empresarial",
    "Participação em processo licitatório",
    "Atualização cadastral em órgão público",
    "Publicação de balanço patrimonial",
    "Registro de alteração contratual",
]

TITULOS_POSITIVO = [
    "Empresa reconhecida por boas práticas",
    "Prêmio de sustentabilidade concedido",
    "Certificação de qualidade obtida",
    "Destaque em ranking setorial",
    "Participação em programa de compliance",
]

DESCRICOES_NEGATIVO = [
    "Veiculação de notícia sobre possível envolvimento em irregularidade "
    "fiscal ou operacional.",
    "Publicação em diário oficial referente a sanção administrativa aplicada.",
    "Menção em relatório de auditoria com indicação de não conformidade.",
    "Reportagem sobre investigação de desvio de energia elétrica na região.",
]

DESCRICOES_NEUTRO = [
    "Registro público de atividade regular sem indicação de irregularidade.",
    "Menção em ato administrativo de rotina sem conotação negativa.",
    "Publicação de edital envolvendo a entidade.",
]

DESCRICOES_POSITIVO = [
    "Reconhecimento público por programa de eficiência energética.",
    "Menção positiva em relatório de responsabilidade social.",
    "Destaque em publicação setorial.",
]


class OsintEventsGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        ext = self.config.get("external", {})
        osint_rate = ext.get("osint_rate", 0.04)
        sanction_boost = ext.get("osint_sanction_boost", 0.70)

        customer_df = self.ctx["customer"]
        sanctions_df = self.ctx.get("sanctions")

        sanction_cpfs: set = set()
        if sanctions_df is not None and len(sanctions_df) > 0:
            sanction_cpfs = set(sanctions_df["cpf_cnpj"].dropna().unique())

        sanctioned = customer_df[customer_df["cpf_cnpj"].isin(sanction_cpfs)]
        non_sanctioned = customer_df[~customer_df["cpf_cnpj"].isin(sanction_cpfs)]

        n_sanction_osint = int(len(sanctioned) * sanction_boost)
        n_normal_osint = int(len(non_sanctioned) * osint_rate)

        sanction_sample = (
            sanctioned.sample(
                n=min(n_sanction_osint, len(sanctioned)),
                random_state=self.seed,
            )
            if len(sanctioned) > 0
            else pd.DataFrame()
        )
        normal_sample = (
            non_sanctioned.sample(
                n=min(n_normal_osint, len(non_sanctioned)),
                random_state=self.seed,
            )
            if len(non_sanctioned) > 0
            else pd.DataFrame()
        )

        entities = pd.concat([sanction_sample, normal_sample], ignore_index=True)

        rows = []
        for i, (_, entity) in enumerate(entities.iterrows()):
            cpf_cnpj = entity["cpf_cnpj"]
            is_sanctioned = cpf_cnpj in sanction_cpfs

            if is_sanctioned:
                sentimento = random.choices(
                    ["NEGATIVO", "NEUTRO", "POSITIVO"],
                    weights=[70, 25, 5],
                    k=1,
                )[0]
                fonte = random.choices(
                    FONTES, weights=[20, 5, 5, 70], k=1
                )[0]
            else:
                sentimento = random.choices(
                    ["NEGATIVO", "NEUTRO", "POSITIVO"],
                    weights=[15, 60, 25],
                    k=1,
                )[0]
                fonte = random.choices(
                    FONTES, weights=[35, 20, 30, 15], k=1
                )[0]

            if sentimento == "NEGATIVO":
                titulo = random.choice(TITULOS_NEGATIVO)
                descricao = random.choice(DESCRICOES_NEGATIVO)
            elif sentimento == "NEUTRO":
                titulo = random.choice(TITULOS_NEUTRO)
                descricao = random.choice(DESCRICOES_NEUTRO)
            else:
                titulo = random.choice(TITULOS_POSITIVO)
                descricao = random.choice(DESCRICOES_POSITIVO)

            rows.append({
                "event_id": f"OSINT-{i + 1:06d}",
                "cpf_cnpj": cpf_cnpj,
                "fonte": fonte,
                "titulo": titulo,
                "descricao": descricao,
                "data": self.random_date().strftime("%Y-%m-%d"),
                "sentimento": sentimento,
            })

        df = pd.DataFrame(rows)
        self.save(df, "osint_events")
        self.ctx["osint_events"] = df
        return df
