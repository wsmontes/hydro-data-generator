"""
consumer_unit.py — Gerador da tabela `consumer_unit` (master central).

Regras de negócio implementadas:
- tipo_ligacao restrito por perfil (residencial nunca tem AT)
- PJ (CNPJ) nunca é residencial
- CNAE segregado por tipo de consumidor (rural → agro, comercial → comércio/serviços, etc.)
- installation_number não-sequencial (formato realístico de distribuidora)
- perfil_consumo e is_fraud são campos internos — não exportados
"""

from __future__ import annotations

import random

import numpy as np
import pandas as pd

from generators.base import BaseGenerator, CONSUMER_PROFILES
from generators.static import CNAE_CODES_COMERCIAL, CNAE_CODES_INDUSTRIAL, CNAE_CODES_RURAL

# ---------------------------------------------------------------------------
# Tipo de ligação por perfil de consumo
# Regra ANEEL/distribuidoras brasileiras:
#   Grupo B (BT) → mono, bi, tri (tensão ≤ 2,3 kV)
#   Grupo A (AT) → exclusivo para grandes consumidores com subestação própria
# ---------------------------------------------------------------------------

LIGACAO_BY_PROFILE: dict[str, tuple[list, list]] = {
    # (opções, pesos)
    # N2: residencial é sempre Grupo B1 (ANEEL) — mono ou bifásico, nunca trifásico
    "residencial_baixo":   (["monofasico", "bifasico"],  [80, 20]),
    "residencial_medio":   (["monofasico", "bifasico"],  [65, 35]),
    "residencial_alto":    (["monofasico", "bifasico"],  [50, 50]),
    "comercial_pequeno":   (["monofasico", "bifasico"],                    [55, 45]),
    "comercial_medio":     (["bifasico",   "trifasico"],                   [30, 70]),
    "industrial":          (["trifasico",  "AT"],                          [45, 55]),
    "rural":               (["monofasico", "bifasico"],                    [80, 20]),
}

STATUS_OPTIONS = ["ativo", "ativo", "ativo", "ativo", "suspenso", "cortado"]

# Prefixos regionais de instalação por cidade (simulando rotas de leitura)
# Número de instalação = prefixo (3 dígitos) + sequencial embaralhado (7 dígitos)
REGIONAL_PREFIXES = {
    "Campinas":              ["301", "302", "303", "304"],
    "Piracicaba":            ["412", "413"],
    "Limeira":               ["523", "524"],
    "Americana":             ["631", "632"],
    "Sumaré":                ["741"],
    "Santa Bárbara d'Oeste": ["851"],
}


class ConsumerUnitGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        n = self.config["volumes"]["consumer_units"]
        fraud_rate = self.config["fraud"]["fraud_rate"]
        n_fraud = int(n * fraud_rate)

        transformer_ids = list(self.ctx["transformer"]["transformer_id"])
        # E3: peso proporcional à capacidade → transformadores maiores recebem mais UCs
        transformer_weights = list(self.ctx["transformer"]["capacidade_kva"])
        address_df = self.ctx["address"]
        customer_df = self.ctx["customer"]

        # Separa pool de documentos por tipo
        pf_pool  = list(customer_df.loc[customer_df["tipo"] == "PF",  "cpf_cnpj"])
        pj_pool  = list(customer_df.loc[customer_df["tipo"] == "PJ",  "cpf_cnpj"])

        # ~5% de clientes com mais de 1 UC (para análise de rede)
        multi_pool = random.sample(pf_pool, k=max(1, int(len(pf_pool) * 0.05)))
        multi_cpfs = {c: random.randint(2, 3) for c in multi_pool}
        multi_entries: list[str] = []
        for cpf, count in multi_cpfs.items():
            multi_entries.extend([cpf] * count)
        remaining_pf = [c for c in pf_pool if c not in multi_cpfs]

        # Perfis de consumidor com pesos
        profiles = list(CONSUMER_PROFILES.keys())
        weights  = [CONSUMER_PROFILES[p]["weight"] for p in profiles]

        # Datas de ligação espalhadas nos 10 anos anteriores ao início da série
        date_range = pd.date_range(
            start=self.date_start - pd.DateOffset(years=10),
            end=self.date_start,
            freq="D",
        )

        fraud_indices = set(random.sample(range(n), k=n_fraud))

        used_inst_nums: set = set()
        rows = []
        for i in range(n):
            profile        = random.choices(profiles, weights=weights, k=1)[0]
            tipo_consumidor = _profile_to_tipo(profile)

            # ---------- tipo de ligação por perfil (C1) ----------
            opts, wts = LIGACAO_BY_PROFILE[profile]
            tipo_ligacao = random.choices(opts, weights=wts, k=1)[0]

            # ---------- CPF/CNPJ coerente com tipo (C2) ----------
            cpf_cnpj = _assign_doc(
                tipo_consumidor, i, multi_entries, remaining_pf, pj_pool
            )

            # ---------- CNAE por tipo de consumidor (C3) ----------
            cnae = _assign_cnae(tipo_consumidor)

            # ---------- endereço ----------
            addr_row = address_df.sample(1).iloc[0]
            addr_id  = addr_row["endereco_id"]
            cidade   = addr_row["cidade"]

            # ---------- número de instalação não-sequencial e único (C5) ----------
            prefix   = random.choice(REGIONAL_PREFIXES.get(cidade, ["999"]))
            inst_num = ""
            while not inst_num or inst_num in used_inst_nums:
                suffix   = self.rng.integers(1000000, 9999999)
                inst_num = f"{prefix}{suffix}"
            used_inst_nums.add(inst_num)

            rows.append({
                "consumer_id":         f"UC-{i + 1:06d}",
                "installation_number": inst_num,
                "cpf_cnpj":            cpf_cnpj,
                "tipo_consumidor":     tipo_consumidor,
                "tipo_ligacao":        tipo_ligacao,
                "transformador_id":    random.choices(transformer_ids, weights=transformer_weights, k=1)[0],
                "endereco_id":         addr_id,
                "cnae":                cnae,
                "data_ligacao":        random.choice(date_range).strftime("%Y-%m-%d"),
                "status":              random.choice(STATUS_OPTIONS),
                # campos internos (não exportados)
                "perfil_consumo":      profile,
                "is_fraud":            i in fraud_indices,
            })

        df = pd.DataFrame(rows)

        # R3: garantir que todo transformador tenha ao menos 1 UC atribuída
        # (evita nós isolados no grafo DataWalk)
        assigned_trs = set(df["transformador_id"])
        for tr_id in transformer_ids:
            if tr_id not in assigned_trs:
                # R3: elege UC de um TR com ≥2 UCs → evita criar novo órfão
                tr_counts = df["transformador_id"].value_counts()
                safe_trs  = tr_counts[tr_counts >= 2].index.tolist()
                src_tr    = random.choice(safe_trs)
                idxs      = df.index[df["transformador_id"] == src_tr].tolist()
                idx       = random.choice(idxs)
                df.at[idx, "transformador_id"] = tr_id
                assigned_trs.add(tr_id)

        # Armazena no contexto COM os campos internos (outros geradores precisam)
        self.ctx["consumer_unit_full"] = df

        # Q2: remove clientes órfãos — customer.csv só deve conter CPF/CNPJs
        # efetivamente referenciados por ao menos 1 UC (evita 191 registros sem UC)
        used_cpfs = set(df["cpf_cnpj"].dropna())
        customer_synced = (
            customer_df[customer_df["cpf_cnpj"].isin(used_cpfs)]
            .reset_index(drop=True)
        )
        self.save(customer_synced, "customer")
        self.ctx["customer"] = customer_synced

        # Exporta sem campos internos (C4: perfil_consumo fora; is_fraud fora)
        export_df = df.drop(columns=["perfil_consumo", "is_fraud"])
        self.save(export_df, "consumer_unit")
        return df  # retorna completo para o contexto


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def _profile_to_tipo(profile: str) -> str:
    if "residencial" in profile:
        return "residencial"
    if "comercial" in profile:
        return "comercial"
    if "industrial" in profile:
        return "industrial"
    return "rural"


def _assign_doc(
    tipo_consumidor: str,
    idx: int,
    multi_entries: list[str],
    remaining_pf: list[str],
    pj_pool: list[str],
) -> str:
    """
    Garante coerência entre tipo_consumidor e tipo de documento.
    - residencial → sempre CPF (PF)
    - industrial   → 80% CNPJ, 20% CPF
    - comercial    → 50% CNPJ, 50% CPF
    - rural        → 90% CPF, 10% CNPJ (cooperativas)
    """
    if tipo_consumidor == "residencial":
        # multi_entries têm prioridade para garantir UCs com mesmo titular
        if idx < len(multi_entries):
            return multi_entries[idx]
        return random.choice(remaining_pf) if remaining_pf else "000.000.000-00"

    pj_probs = {"industrial": 0.80, "comercial": 0.50, "rural": 0.10}
    use_pj = random.random() < pj_probs.get(tipo_consumidor, 0.50)
    if use_pj and pj_pool:
        return random.choice(pj_pool)
    return random.choice(remaining_pf) if remaining_pf else random.choice(pj_pool)


def _assign_cnae(tipo_consumidor: str) -> str | None:
    if tipo_consumidor == "residencial":
        return None
    if tipo_consumidor == "comercial":
        return random.choice(CNAE_CODES_COMERCIAL)
    if tipo_consumidor == "industrial":
        return random.choice(CNAE_CODES_INDUSTRIAL)
    if tipo_consumidor == "rural":
        return random.choice(CNAE_CODES_RURAL)
    return None
