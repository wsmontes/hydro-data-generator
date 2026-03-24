Boa, agora a gente sai do “o que analisar” e entra no **como isso existe de verdade dentro da empresa** — que normalmente é um caos distribuído em sistemas diferentes (billing, medição, GIS, CRM, field service, etc).

Vou estruturar em 3 camadas:

1. **Como os dados costumam estar armazenados (tabelas reais)**
2. **Quais deveriam estar juntas vs separadas**
3. **Quais joins/cruzamentos você vai precisar (isso aqui é o ouro pro DataWalk)**

---

# 🧱 1. COMO ISSO EXISTE NA VIDA REAL (TABELAS)

## 🧍‍♂️ 1.1 Unidade Consumidora (MASTER)

**Tabela: `consumer_unit`**

* consumer_id (PK)
* installation_number
* cpf_cnpj
* tipo_consumidor (residencial, comercial…)
* tipo_ligacao (mono/bi/tri/AT)
* transformador_id
* endereco_id
* cnae
* data_ligacao
* status

👉 Essa é a **tabela central de tudo**

---

## ⚡ 1.2 Medições (Time Series)

**Tabela: `meter_reading`**

* reading_id (PK)
* consumer_id (FK)
* data_leitura
* data_faturamento
* consumo_kwh
* demanda_kw
* dias_entre_leituras
* consumo_normalizado_30d

👉 Altamente volumosa (milhões/bilhões de linhas)

---

## 📝 1.3 Ocorrências de Leitura

**Tabela: `reading_occurrence`**

* occurrence_id
* reading_id (FK)
* tipo_ocorrencia:

  * imóvel fechado
  * média
  * suspeita fraude
* observacao

---

## 📸 1.4 Evidências (Imagem do medidor)

**Tabela: `meter_image`**

* image_id
* reading_id (FK)
* path_arquivo / blob
* timestamp

---

## 🚨 1.5 Inspeções / TOI

**Tabela: `inspection`**

* toi_id
* consumer_id
* data_inspecao
* tipo_irregularidade (fraude/desvio)
* resultado
* valor_recuperado
* reincidente_flag

---

## 🔌 1.6 Transformadores

**Tabela: `transformer`**

* transformer_id
* localizacao
* capacidade
* tipo

---

## ⚡ 1.7 Medição do Transformador

**Tabela: `transformer_reading`**

* reading_id
* transformer_id
* data
* energia_total

---

## 🏗️ 1.8 Carga Declarada

**Tabela: `declared_load`**

* consumer_id
* equipamento
* potencia_kw
* horas_dia
* data_declaracao

---

## 🏢 1.9 Atividade Econômica

**Tabela: `economic_activity`**

* cnae
* descricao
* consumo_medio_categoria

---

## 👷 1.10 Ordens de Serviço (Eletricista)

**Tabela: `work_order`**

* order_id
* consumer_id
* eletricista_id
* data_execucao
* tipo_servico

---

## 👨‍🔧 1.11 Eletricistas

**Tabela: `electrician`**

* eletricista_id
* nome
* empresa
* status

---

## 📟 1.12 Leituristas

**Tabela: `meter_reader`**

* reader_id
* nome

---

## 📋 1.13 Quem fez a leitura

**Tabela: `reading_agent`**

* reading_id
* reader_id

---

## 🧾 1.14 Cliente (Pessoa)

**Tabela: `customer`**

* cpf_cnpj
* nome
* tipo

---

## 🏠 1.15 Endereço / Região

**Tabela: `address`**

* endereco_id
* bairro
* cidade
* renda_media_regiao

---

## 🗺️ 1.16 Dados externos (ex: Google Maps)

**Tabela: `external_property_data`**

* consumer_id
* area_construida
* data_imagem
* fonte

---

# 🧩 2. O QUE DEVERIA ESTAR JUNTO vs SEPARADO

## 🔗 Devem estar juntos (mesma tabela ou fortemente acoplados)

### ✔️ Medição + ocorrência

→ `meter_reading` + `reading_occurrence`

👉 porque:

* análise sempre usa os dois juntos
* evita join pesado em série temporal

---

### ✔️ Unidade consumidora + transformador_id

→ já está embutido

👉 essencial pra análise de perda por transformador

---

### ✔️ Medição + agente (leiturista)

→ idealmente embedado ou view materializada

---

## 🧱 Devem ficar separados

### ❌ Medição vs inspeção

* naturezas diferentes (tempo vs evento)

---

### ❌ Cliente vs unidade consumidora

* 1 cliente pode ter várias unidades

---

### ❌ Carga declarada vs consumo real

* modelos diferentes (estimado vs observado)

---

### ❌ Transformador vs consumidores

* relação 1:N dinâmica

---

# 🔀 3. CRUZAMENTOS (ESSA É A PARTE MAIS IMPORTANTE)

Aqui é literalmente o que você vai implementar no DataWalk.

---

## ⚡ 3.1 Balanço por Transformador

```sql
transformer_reading
JOIN consumer_unit
JOIN meter_reading
```

👉 lógica:

* soma(consumo consumidores)
* vs leitura transformador

---

## 📉 3.2 Detecção de queda gradual

```sql
meter_reading (self join por tempo)
```

👉 precisa:

* janela temporal (window functions)
* tendência

---

## 🔁 3.3 Reincidência

```sql
inspection
JOIN consumer_unit
JOIN customer (cpf/cnpj)
```

👉 pega:

* múltiplas unidades do mesmo dono

---

## 🧠 3.4 Sazonalidade (baseline)

```sql
meter_reading
JOIN consumer_unit
JOIN economic_activity
JOIN address
```

👉 cluster por:

* região
* tipo
* CNAE

---

## ⚖️ 3.5 Consumo vs carga declarada

```sql
declared_load
JOIN consumer_unit
JOIN meter_reading
```

👉 calcula:

* consumo esperado vs real

---

## 🏗️ 3.6 Mudança estrutural (Google Maps)

```sql
external_property_data
JOIN consumer_unit
JOIN meter_reading
```

👉 detecta:

* aumento área vs consumo parado

---

## 👷 3.7 Corrupção – eletricistas

```sql
work_order
JOIN consumer_unit
JOIN meter_reading
```

👉 padrão:

* queda após visita

---

## 📟 3.8 Corrupção – leiturista

```sql
reading_agent
JOIN meter_reading
```

👉 padrão:

* leituras suspeitas concentradas

---

## 🧑‍🤝‍🧑 3.9 Rede de fraude (grafo)

```sql
consumer_unit
JOIN customer
JOIN inspection
JOIN electrician
```

👉 cria:

* clusters de fraude

---

## 🏘️ 3.10 Anomalia por região rica

```sql
consumer_unit
JOIN address
JOIN meter_reading
```

👉 compara:

* consumo vs renda média da área

---

# 🧠 4. COMO ISSO VIRA DATAWALK (INSIGHT IMPORTANTE)

No DataWalk você NÃO quer só tabelas.

Você quer:

### Entidades:

* Consumer Unit
* Meter Reading
* Transformer
* Person (CPF/CNPJ)
* Electrician
* Meter Reader
* Inspection
* Region

---

### Links:

* CONSUMES_FROM (consumer → transformer)
* OWNED_BY (consumer → person)
* READ_BY (reading → reader)
* SERVICED_BY (consumer → electrician)
* INSPECTED (consumer → inspection)

---

### Métricas derivadas (super importante):

* consumo normalizado
* delta consumo
* score de anomalia
* risco por transformador
* score de fraude por cluster

---

# 💡 Insight final (o mais importante)

O valor aqui NÃO está nas tabelas.

Está nesses 3 tipos de cruzamento:

1. **Tempo (histórico → tendência)**
2. **Hierarquia (transformador → consumidores)**
3. **Rede (pessoas, eletricistas, leituristas)**

👉 Se você montar isso bem no DataWalk, você não só detecta fraude…
você descobre **organização criminosa interna + externa**.

