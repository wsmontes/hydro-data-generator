# DataWalk Cases for Hydro Output

This document explains how the generator places fraud patterns, issues, and investigation signals into the exported dataset in `output/`, and which cases are worth creating in DataWalk.

Scope of this document:

- Dataset analyzed: `output/` generated from `config.yaml`
- Date of analysis: 2026-04-08
- Important limitation: the internal field `is_fraud` is intentionally not exported to `consumer_unit.csv`. In DataWalk, fraud must be inferred from the exported evidence.

## 1. Ground Truth vs Exported Evidence

The generator first assigns hidden fraud labels to consumer units and then propagates those labels into downstream tables.

Internal hidden step:

- `consumer_unit_full.is_fraud` is assigned to `int(consumer_units * fraud_rate)` units.
- With the current `config.yaml`, that means `500 * 0.08 = 40` hidden fraud UCs.
- This flag is not exported to `consumer_unit.csv`.

What appears in the output instead:

- abnormal time series in `meter_reading.csv`
- suspicious reader allocation in `reading_agent.csv`
- suspicious reading events in `reading_occurrence.csv`
- fraud-oriented work orders in `work_order.csv`
- confirmed irregularities and recurrence in `inspection.csv`
- elevated technical-loss style signals in `transformer_reading.csv`
- graph structure through shared `cpf_cnpj` in `consumer_unit.csv`

This means DataWalk should be case-driven and pattern-driven, not label-driven.

## 2. Exactly Where Each Signal Was Inserted

### 2.1 Hidden fraud selection

How it is inserted:

- `ConsumerUnitGenerator` randomly selects 40 out of 500 UCs as internal frauds.
- The hidden field is stored only in memory as `consumer_unit_full.is_fraud`.

Where it surfaces in exported output:

- not visible directly in `consumer_unit.csv`
- visible indirectly through the downstream patterns listed below

DataWalk implication:

- do not expect a direct fraud label column
- treat each case as a hypothesis assembled from multiple tables

### 2.2 Gradual consumption drop

How it is inserted:

- A subset of hidden fraud UCs receives the `gradual_drop` pattern.
- Current config: `gradual_drop_pct = 0.40`
- Approximately 40% of hidden fraud UCs receive a month-by-month reduction in expected consumption.
- The drop reaches up to 60% by the end of the series.

Where it appears:

- `meter_reading.csv`
- columns: `consumer_id`, `data_leitura`, `consumo_kwh`, `consumo_normalizado_30d`

How to detect it:

- compare early vs late monthly average consumption per `consumer_id`
- a strong signature is a drop of at least 45% between the first and last months of the series

What exists in the current output:

- 15 consumers already show a strong gradual-drop signature

Best DataWalk case type:

- case: `UC with persistent consumption collapse`
- primary entity: `consumer_id`
- supporting evidence: monthly readings and normalized monthly consumption

### 2.3 Reader corruption pattern

How it is inserted:

- The generator marks the first `1/8` of all meter readers as potentially corrupt.
- A subset of hidden fraud UCs is routed more often to those readers.
- Current config: `reader_corruption_pct = 0.20`
- When the pattern is active, many readings are reduced by roughly 20% to 40%.

Where it appears:

- `reading_agent.csv` links `reading_id -> reader_id`
- `meter_reading.csv` provides `reading_id`, `consumer_id`, `consumo_kwh`

How to detect it:

- compute average `consumo_kwh` by `reader_id`
- compare with global mean
- suspicious readers are those consistently below the fleet average

What exists in the current output:

- 5 readers are below 97% of the global mean
- strongest low-average readers in the current output:
  - `READ-0001`: 90.79% of the global mean
  - `READ-0007`: 92.89% of the global mean
  - `READ-0012`: 93.47% of the global mean

Best DataWalk case type:

- case: `Suspicious meter reader`
- primary entity: `reader_id`
- supporting evidence: linked readings, impacted consumers, mean consumption deviation

### 2.4 Suspicious reading occurrences

How it is inserted:

- Every reading has a base chance of creating an occurrence.
- Current config: `reading_occurrence_rate = 0.10`
- Fraud-linked consumers receive a 3x higher occurrence probability.
- Fraud-linked consumers also have a 40% chance of being forced into `tipo_ocorrencia = suspeita_fraude` when an occurrence is generated.

Where it appears:

- `reading_occurrence.csv`
- columns: `occurrence_id`, `reading_id`, `tipo_ocorrencia`, `observacao`
- joined to `meter_reading.csv` through `reading_id`

Important actual values in the current export:

- `suspeita_fraude`
- `acesso_negado`
- `relogio_parado`
- `medidor_danificado`
- `leitura_estimada`
- `imovel_fechado`

Important free-text clues already present in `observacao`:

- `Lacre com sinais de violação.`
- `Medidor com leitura incompatível com histórico.`
- `Identificado bypass externo.`

What exists in the current output:

- 2,112 total reading occurrences
- 388 occurrences of type `suspeita_fraude`

Best DataWalk case type:

- case: `Reading with fraud suspicion`
- primary entity: `reading_id` or `consumer_id`
- supporting evidence: occurrence type, observation text, linked consumption behavior

### 2.5 Electrician correlation

How it is inserted:

- The first 20% of active electricians are treated as suspect electricians.
- A subset of hidden fraud UCs is correlated with those electricians.
- Current config: `electrician_correlation_pct = 0.30`
- For those consumers, work orders are more likely to be handled by suspect electricians.
- Work-order execution is often placed 1 to 14 days before a reading, creating a temporal link between a field visit and low subsequent consumption.

Where it appears:

- `work_order.csv`
- columns: `consumer_id`, `eletricista_id`, `data_execucao`, `tipo_servico`
- linked to `meter_reading.csv` by `consumer_id` and time proximity

How to detect it:

- find readings up to 7 days after a work order
- compare those readings with the global or peer-group average
- rank electricians by number of low-consumption readings after a visit

What exists in the current output:

- 803 work orders total
- 43 work orders of type `vistoria_fraude`
- strongest electricians by low readings within 7 days after service:
  - `ELET-0020`: 22 low readings across 22 consumers
  - `ELET-0002`: 21 low readings across 20 consumers
  - `ELET-0003`: 15 low readings across 14 consumers

Best DataWalk case type:

- case: `Electrician linked to post-visit consumption drops`
- primary entity: `eletricista_id`
- supporting evidence: work order timeline, affected consumers, post-visit readings

### 2.6 Fraud work orders and inspections

How it is inserted:

- Every hidden fraud UC is forced to receive at least one work order.
- The first work order for a fraud UC is forced to be `vistoria_fraude`.
- Inspection generation heavily favors hidden fraud UCs.
- Hidden fraud UCs are more likely to produce `resultado = irregularidade_confirmada`.
- Repeated inspections on the same UC produce `reincidente_flag = True` after a prior confirmation.

Where it appears:

- `work_order.csv`
- `inspection.csv`

Important inspection fields:

- `resultado`
- `tipo_irregularidade`
- `valor_recuperado`
- `reincidente_flag`

Important actual irregularity values in the current output:

- `desvio_direto`
- `adulteracao_medidor`
- `ligacao_clandestina`
- `bypass`
- `adulteracao_lacre`

What exists in the current output:

- 80 inspections total
- 36 inspections with `resultado = irregularidade_confirmada`
- 8 inspections with `reincidente_flag = True`
- 11 consumers have multiple inspections

Best DataWalk case types:

- case: `UC with confirmed irregularity`
- case: `UC with repeated confirmed behavior`
- case: `UC with vistoria_fraude followed by inspection`

### 2.7 Transformer loss hotspot

How it is inserted:

- Consumer readings are summed by transformer and month.
- A loss factor is then applied.
- Transformers with no fraud density get roughly 3% to 7% loss.
- Transformers with some hidden-fraud density get roughly 7% to 12% loss.
- Transformers with higher hidden-fraud density get roughly 10% to 15% loss.

Where it appears:

- `transformer_reading.csv`
- columns: `transformer_id`, `energia_total_kwh`, `soma_consumidores_kwh`, `perda_estimada_pct`
- linked to `consumer_unit.csv` through `transformador_id`

What exists in the current output:

- 410 transformer monthly readings above 10% estimated loss
- 6 transformers have average estimated loss above 10%
- top average-loss transformers:
  - `TR-0008`: 12.96%
  - `TR-0034`: 12.60%
  - `TR-0017`: 12.59%
  - `TR-0004`: 12.44%
  - `TR-0022`: 12.32%

Best DataWalk case type:

- case: `High-loss transformer cluster`
- primary entity: `transformer_id`
- supporting evidence: monthly loss trend and linked consumers under the transformer

### 2.8 Ownership or network clusters

How it is inserted:

- Around 5% of PF customers are replicated across multiple consumer units.
- This creates graph structure through shared `cpf_cnpj`.
- This is the actual implemented ownership cluster mechanism.

Where it appears:

- `consumer_unit.csv`
- columns: `consumer_id`, `cpf_cnpj`, `transformador_id`, `endereco_id`

What exists in the current output:

- 129 documents are linked to 2 or more consumer units
- strongest examples:
  - `153.860.247-44` linked to 6 UCs
  - `120.643.795-25` linked to 5 UCs
  - `960.354.281-42` linked to 5 UCs
  - `809.671.542-94` linked to 5 UCs
  - `453.210.967-16` linked to 4 UCs

Best DataWalk case type:

- case: `Shared owner with multiple suspicious units`
- primary entity: `cpf_cnpj`
- supporting evidence: linked UCs, transformer overlap, inspection outcomes, suspicious readings

## 3. Current Best Seed Cases for DataWalk

If you want the strongest cases first, generate them from consumers that already carry multiple linked signals.

### Priority 1: compound consumer cases

These are the best seed investigations because they combine several exported signals.

- `UC-000024`: gradual drop, suspicious occurrence, `vistoria_fraude`, confirmed irregularity, recurrence, shared owner
- `UC-000339`: gradual drop, suspicious occurrence, `vistoria_fraude`, confirmed irregularity, recurrence, shared owner
- `UC-000036`: gradual drop, suspicious occurrence, `vistoria_fraude`, confirmed irregularity, shared owner
- `UC-000064`: gradual drop, suspicious occurrence, `vistoria_fraude`, confirmed irregularity, shared owner
- `UC-000190`: suspicious occurrence, `vistoria_fraude`, confirmed irregularity, recurrence, shared owner

There are 27 consumers in the current output with at least 4 of the following signals:

- gradual drop
- `suspeita_fraude` occurrence
- `vistoria_fraude` work order
- confirmed irregularity
- recurrence flag
- shared owner cluster

### Priority 2: actor-centric cases

Create cases around suspicious actors because they can connect many consumers.

- suspicious readers: `READ-0001`, `READ-0007`, `READ-0012`
- suspicious electricians: `ELET-0020`, `ELET-0002`, `ELET-0003`

These cases are valuable because they let DataWalk surface many-to-many patterns instead of isolated UCs.

### Priority 3: infrastructure cases

Create transformer-centric cases for loss concentration.

- `TR-0008`
- `TR-0034`
- `TR-0017`
- `TR-0004`
- `TR-0022`

These are good starting points for cases that pivot from grid losses to suspicious consumers, readers, inspections, and owners.

## 4. Recommended DataWalk Case Types

Generate these case families in order of usefulness.

### Case A. Consumer fraud hypothesis

Create one case per `consumer_id` when at least 2 of these are true:

- strong gradual drop in `meter_reading`
- one or more `suspeita_fraude` occurrences
- one or more `vistoria_fraude` work orders
- one or more confirmed inspections
- linked to a high-loss transformer

Core joins:

- `consumer_unit.consumer_id = meter_reading.consumer_id`
- `meter_reading.reading_id = reading_occurrence.reading_id`
- `consumer_unit.consumer_id = work_order.consumer_id`
- `consumer_unit.consumer_id = inspection.consumer_id`
- `consumer_unit.transformador_id = transformer_reading.transformer_id`

### Case B. Suspicious meter reader

Create one case per `reader_id` when:

- mean consumption of linked readings is materially below peer readers
- the reader is linked to many consumers with suspicious occurrences or inspections

Core joins:

- `reading_agent.reading_id = meter_reading.reading_id`
- `meter_reading.consumer_id = consumer_unit.consumer_id`
- `meter_reading.reading_id = reading_occurrence.reading_id`

### Case C. Suspicious electrician

Create one case per `eletricista_id` when:

- the electrician appears before repeated low readings
- the electrician is associated with `vistoria_fraude`
- the linked consumers later receive confirmed inspections

Core joins:

- `work_order.consumer_id = meter_reading.consumer_id`
- `work_order.consumer_id = inspection.consumer_id`

### Case D. Shared-owner network

Create one case per `cpf_cnpj` when:

- 2 or more UCs share the same document
- multiple linked UCs carry suspicious signals
- those UCs also overlap by transformer, region, or actor

Core joins:

- `consumer_unit.cpf_cnpj` as graph hub
- then pivot to `meter_reading`, `work_order`, `inspection`, and `transformer_reading`

### Case E. High-loss transformer

Create one case per `transformer_id` when:

- average `perda_estimada_pct` is above 10%
- there are multiple linked UCs with suspicious occurrences, confirmed inspections, or suspect actors

Core joins:

- `consumer_unit.transformador_id = transformer_reading.transformer_id`
- `consumer_unit.consumer_id = inspection.consumer_id`
- `consumer_unit.consumer_id = meter_reading.consumer_id`

## 5. What Is Configured but Not Really Implemented

These points matter because they affect what you should and should not model as a DataWalk case.

### 5.1 `fraud.network_clusters`

The config contains `network_clusters: 3`, but the current code does not use that parameter to create explicit fraud clusters.

What is actually generated instead:

- repeated `cpf_cnpj` ownership links across consumer units

Practical meaning:

- DataWalk can already model ownership clusters
- but it should not assume there are exactly 3 explicit fraud clusters generated by code

### 5.2 `fraud.recurrence_rate`

The config contains `recurrence_rate: 0.10`, but the current inspection generation does not use that parameter to control recurrence volume.

What is actually generated instead:

- all hidden fraud UCs are placed in the inspection pool
- then an extra fixed 30% duplicate sampling is added for repeated inspections

Practical meaning:

- recurrence exists in the output
- but its volume is not currently controlled by the `recurrence_rate` knob

### 5.3 No direct fraud label in exports

This is intentional and should be preserved in DataWalk.

Practical meaning:

- cases should be investigative
- scores should be evidence-based
- do not build a pipeline that expects a ground-truth label from CSV import

## 6. Minimal DataWalk Import Model

If you want a lean but effective import, start with these entities:

- Consumer Unit
- Customer Document (`cpf_cnpj`)
- Meter Reading
- Reading Occurrence
- Meter Reader
- Work Order
- Electrician
- Inspection
- Transformer
- Transformer Reading

And these relationships:

- Customer Document `OWNS` Consumer Unit
- Consumer Unit `HAS_READING` Meter Reading
- Meter Reading `HAS_OCCURRENCE` Reading Occurrence
- Meter Reading `READ_BY` Meter Reader
- Consumer Unit `HAS_WORK_ORDER` Work Order
- Work Order `EXECUTED_BY` Electrician
- Consumer Unit `HAS_INSPECTION` Inspection
- Consumer Unit `FED_BY` Transformer
- Transformer `HAS_READING` Transformer Reading

## 7. Recommended First Case Load

If you want a practical first batch in DataWalk, start with:

1. All consumers with confirmed inspections
2. All consumers with `vistoria_fraude`
3. All consumers with `suspeita_fraude` occurrences
4. All consumers with strong gradual drops
5. Shared-document clusters where at least one UC has a confirmed inspection
6. Readers with average consumption below 97% of global mean
7. Electricians with many low-consumption readings within 7 days after work orders
8. Transformers with average `perda_estimada_pct > 10%`

That order gives you a mix of direct evidence, temporal patterns, network structure, and infrastructure anomalies.