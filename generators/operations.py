"""
operations.py — Geradores de: work_order e inspection.

Padrões de fraude:
  - work_order: eletricistas correlacionados → queda de consumo após visita
  - inspection: reincidência de UCs fraudulentas
"""

from __future__ import annotations

import random

import pandas as pd

from generators.base import BaseGenerator

TIPOS_SERVICO = [
    "troca_medidor",
    "religacao",
    "inspecao_rotina",
    "manutencao_rede",
    "substituicao_poste",
    "vistoria_fraude",
    "corte_por_debito",
]

TIPOS_IRREGULARIDADE = [
    "desvio_direto",
    "adulteracao_medidor",
    "ligacao_clandestina",
    "bypass",
    "adulteracao_lacre",
]

RESULTADOS_INSPECAO = [
    "irregularidade_confirmada",
    "sem_irregularidade",
    "inconcluso",
]


class WorkOrderGenerator(BaseGenerator):
    """
    Ordens de serviço por UC.
    UCs fraudulentas têm eletricistas "correlacionados" (suspeitos).
    """

    def generate(self) -> pd.DataFrame:
        cu_df = self.ctx["consumer_unit_full"]
        # O4: apenas eletricistas ATIVOS recebem OS novas
        elec_df = self.ctx["electrician"]
        electricians = list(elec_df.loc[elec_df["status"] == "ativo", "eletricista_id"])
        mr_df = self.ctx["meter_reading_full"]
        fraud_cfg = self.config["fraud"]
        max_orders = self.config["volumes"]["max_work_orders_per_unit"]

        # Eletricistas suspeitos: primeiros 20% da lista
        n_suspect = max(1, int(len(electricians) * 0.20))
        suspect_electricians = electricians[:n_suspect]

        fraud_ids = set(cu_df.loc[cu_df["is_fraud"], "consumer_id"])
        n_elec_corr = int(len(fraud_ids) * fraud_cfg["electrician_correlation_pct"])
        elec_fraud_ucs = set(random.sample(list(fraud_ids), k=min(n_elec_corr, len(fraud_ids))))
        # P3: rastreia quais UCs de fraude já receberam ao menos 1 vistoria_fraude
        fraud_vistoria_done: set[str] = set()

        # Datas de leitura disponíveis por UC (para referenciar visita antes de queda)
        reading_dates = (
            mr_df.groupby("consumer_id")["data_leitura"]
            .apply(list)
            .to_dict()
        )

        rows = []
        order_idx = 1
        for _, uc in cu_df.iterrows():
            cid = uc["consumer_id"]
            # O5: UCs fraudulentas devem ter ao menos 1 OS (suspeita -> OS -> inspeção)
            n_min_orders = 1 if uc["is_fraud"] else 0
            n_orders = random.randint(n_min_orders, max_orders)
            is_elec_fraud = cid in elec_fraud_ucs

            available_dates = reading_dates.get(cid, [])

            is_fraud_uc = cid in fraud_ids

            for _ in range(n_orders):
                # P3: primeira OS de qualquer UC fraudulenta é sempre vistoria_fraude
                force_vistoria = is_fraud_uc and cid not in fraud_vistoria_done

                if is_elec_fraud:
                    eletricista_id = random.choice(suspect_electricians)
                    if force_vistoria:
                        tipo = "vistoria_fraude"
                        fraud_vistoria_done.add(cid)
                    else:
                        tipo = random.choices(
                            TIPOS_SERVICO,
                            weights=[1, 2, 1, 1, 1, 3, 1],
                            k=1,
                        )[0]
                    # Visita ocorre num dia próximo a uma leitura (para detectar correlação)
                    if available_dates:
                        ref_date = pd.to_datetime(random.choice(available_dates))
                        data_exec = ref_date - pd.Timedelta(days=random.randint(1, 14))
                    else:
                        data_exec = self.random_date()
                else:
                    eletricista_id = random.choice(electricians)
                    if force_vistoria:
                        tipo = "vistoria_fraude"
                        fraud_vistoria_done.add(cid)
                    else:
                        # E5: vistoria_fraude só faz sentido para UCs suspeitas
                        tipos_normais = [t for t in TIPOS_SERVICO if t != "vistoria_fraude"]
                        tipo = random.choice(tipos_normais)
                    data_exec = self.random_date()

                rows.append({
                    "order_id": f"WO-{order_idx:07d}",
                    "consumer_id": cid,
                    "eletricista_id": eletricista_id,
                    "data_execucao": pd.Timestamp(data_exec).strftime("%Y-%m-%d"),
                    "tipo_servico": tipo,
                })
                order_idx += 1

        df = pd.DataFrame(rows)
        self.save(df, "work_order")
        return df


class InspectionGenerator(BaseGenerator):
    def generate(self) -> pd.DataFrame:
        cu_df = self.ctx["consumer_unit_full"]
        n_inspections = self.config["volumes"]["inspections"]
        recurrence_rate = self.config["fraud"]["recurrence_rate"]
        fraud_cfg = self.config["fraud"]

        fraud_ids = list(cu_df.loc[cu_df["is_fraud"], "consumer_id"])
        all_normal = list(cu_df.loc[~cu_df["is_fraud"], "consumer_id"])
        # O5: inspeções em UCs normais só fazem sentido se já houve alguma OS
        wo_early = self.ctx.get("work_order")
        if wo_early is not None:
            ucs_com_wo = set(wo_early["consumer_id"])
            normal_ids = [cid for cid in all_normal if cid in ucs_com_wo] or all_normal
        else:
            normal_ids = all_normal

        # 70% das inspeções em UCs suspeitas, 30% aleatórias (investigação normal)
        # P3/E5: toda UC fraude aparece ao menos 1× + ~30% extras para reincidência
        n_extra_reinc = int(len(fraud_ids) * 0.30)
        pool_fraud = list(fraud_ids) + random.choices(fraud_ids, k=n_extra_reinc)
        n_normal_insp = max(n_inspections - len(pool_fraud), 0)
        pool_normal = random.choices(normal_ids, k=min(n_normal_insp, max(len(normal_ids), 1)))
        all_uc_ids = pool_fraud + pool_normal
        random.shuffle(all_uc_ids)

        # Segurança: UCs com vistoria_fraude que escaparam do pool (edge cases)
        wo_df = self.ctx.get("work_order")
        if wo_df is not None:
            vistoria_ucs = set(wo_df.loc[wo_df["tipo_servico"] == "vistoria_fraude", "consumer_id"])
            already_in = set(all_uc_ids)
            for cid in vistoria_ucs:
                if cid not in already_in:
                    all_uc_ids.append(cid)
                    already_in.add(cid)

        random.shuffle(all_uc_ids)

        # Controle de reincidência: seen rastreia inserções NESTA geração
        seen: dict[str, int] = {}

        rows = []
        for i, cid in enumerate(all_uc_ids):
            is_fraud = cid in set(fraud_ids)
            is_reincident = seen.get(cid, 0) >= 1

            # O1: resultado é determinado primeiro; tipo_irregularidade e
            # valor_recuperado derivam dele — nunca o contrário.
            if is_fraud:
                # Q1: reincidência é capturada por reincidente_flag — não há
                # resultado próprio; reincidentes têm maior chance de nova confirmação
                resultado = random.choices(
                    RESULTADOS_INSPECAO,
                    weights=[5, 1, 1] if is_reincident else [4, 1, 1],
                    k=1,
                )[0]
            else:
                resultado = random.choices(
                    ["sem_irregularidade", "inconcluso"],
                    weights=[8, 2],
                    k=1,
                )[0]

            # Derivar tipo_irregularidade e valor_recuperado do resultado
            if resultado == "irregularidade_confirmada":
                tipo_irr  = random.choice(TIPOS_IRREGULARIDADE)
                valor_rec = round(random.uniform(500, 15000), 2)
            elif resultado == "inconcluso":
                # R1: resultado inconclusivo não permite identificar o tipo —
                # tipo_irregularidade só é preenchido em irregularidade_confirmada
                tipo_irr  = None
                valor_rec = 0.0
            else:  # sem_irregularidade
                tipo_irr  = None
                valor_rec = 0.0

            seen[cid] = seen.get(cid, 0) + 1

            rows.append({
                "toi_id": f"TOI-{i + 1:06d}",
                "consumer_id": cid,
                "data_inspecao": self.random_date().strftime("%Y-%m-%d"),
                "tipo_irregularidade": tipo_irr,
                "resultado": resultado,
                "valor_recuperado": valor_rec,
                "reincidente_flag": False,  # preenchido abaixo
            })

        df = pd.DataFrame(rows)
        df["data_inspecao"] = pd.to_datetime(df["data_inspecao"])

        # Q3: inspeção de UC com vistoria_fraude deve ocorrer DEPOIS da OS
        wo_df = self.ctx.get("work_order")
        if wo_df is not None:
            first_vistoria = (
                wo_df[wo_df["tipo_servico"] == "vistoria_fraude"]
                .groupby("consumer_id")["data_execucao"]
                .min()
            )
            first_vistoria = pd.to_datetime(first_vistoria)
            for idx, row in df.iterrows():
                cid = row["consumer_id"]
                if cid in first_vistoria.index:
                    wo_date = first_vistoria[cid]
                    if row["data_inspecao"] <= wo_date:
                        delay = pd.Timedelta(days=int(self.rng.integers(1, 31)))
                        df.at[idx, "data_inspecao"] = wo_date + delay

        df = df.sort_values(["consumer_id", "data_inspecao"]).reset_index(drop=True)

        # E1+E2: consistência temporal de reincidência
        # N3/E1: reincidente_flag=True quando a UC já teve irregularidade confirmada
        # em inspeção anterior (determinístico). Q1: resultado só tem 3 valores.
        first_seen: set[str] = set()
        confirmed_set: set[str] = set()  # UCs com irregularidade já confirmada
        for idx, row in df.iterrows():
            cid = row["consumer_id"]
            if cid not in first_seen:
                first_seen.add(cid)

            res = df.at[idx, "resultado"]
            if cid in confirmed_set:
                df.at[idx, "reincidente_flag"] = True

            if res == "irregularidade_confirmada":
                confirmed_set.add(cid)

        # Renumera toi_id após reordenação
        df["toi_id"] = [f"TOI-{i + 1:06d}" for i in range(len(df))]
        df["data_inspecao"] = df["data_inspecao"].dt.strftime("%Y-%m-%d")

        self.save(df, "inspection")
        return df
