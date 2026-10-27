# Documentação do Pipeline: Bronze e Silver (V-Credit)

Este documento reúne todo o racional, as regras de negócio aplicadas e o guia passo a passo da documentação utilizada nos notebooks de ingestão e tratamento do Case V-Credit, padronizado conforme o padrão de engenharia definido em `Landing_to_Bronze`.

---

## 1. Setup do Ambiente e Ingestão Bronze

### Contexto e Padrão Adotado
Configurar o ambiente analítico no Databricks e realizar a ingestão inicial dos dados brutos diretamente do Volume:
- **Catálogo:** `vcredit`
- **Schemas:** `vcredit.bronze`, `vcredit.silver`, `vcredit.gold`
- **Volume de Entrada:** `/Volumes/vcredit/bronze/Landing`

### Passos Realizados na Bronze:
1. **Setup do Ambiente:**
   - Criação defensiva (`IF NOT EXISTS`) do catálogo (`vcredit`), dos schemas da arquitetura medalhão e do Volume `Landing`.
2. **Ingestão Raw:**
   - Leitura pura via `spark.read.csv(path, header=True, inferSchema=False)` preservando o formato original e sem inferência precoce de tipos.
   - Adição do metadado de auditoria `ingestion_datetime` (`current_timestamp()`).
3. **Garantia de Idempotência:**
   - Escrita em formato **Delta Lake** com `.mode("overwrite")` e `.option("overwriteSchema", True)`:
     - `vcredit.bronze.tb_atendentes`
     - `vcredit.bronze.tb_motivos`
   - O pipeline pode ser reexecutado múltiplas vezes sem inchar a base ou duplicar linhas.

---

## 2. Notebook: `02_Bronze_to_Silver_VCredit`

### Contexto e Objetivo
Leitura da camada Bronze (`vcredit.bronze`) para aplicação de limpeza, tipagem estrita, saneamento de dados faltantes, padronizações e enriquecimento de negócio, gravando na Silver (`vcredit.silver`).

---

### Tabela 1: Atendentes (`vcredit.silver.tb_atendentes`)

#### Diagnóstico da Base Bruta:
- `id_atendente` veio em formato texto puro.
- `nivel_atendimento` com registros inconsistentes misturando `'1'`, `'N1'`, `'Nível 1'`, `'2'`, `'N2'`, `'Nível 2'`.
- `data_admissao` como texto no padrão brasileiro (`dd/MM/yyyy`).
- `data_desligamento` com texto vazio (`""`) para quem continua na empresa.
- `status_atendente` com variações de caixa (`'ativo'`, `'inativo'`).

#### Tratamentos e Regras de Negócio:
1. **Deduplicação Defensiva:** Janela (`Window.partitionBy("id_atendente").orderBy(F.col("ingestion_datetime").desc())`) retendo apenas o registro mais atualizado (`rn == 1`).
2. **Tipagem:** `id_atendente` convertido para `IntegerType()`.
3. **Padronização de Níveis:**
   - `'1'`, `'N1'`, `'Nivel 1'`, `'Nível 1'` $\rightarrow$ **`Nível 1`**
   - `'2'`, `'N2'`, `'Nivel 2'`, `'Nível 2'` $\rightarrow$ **`Nível 2`**
4. **Tratamento de Datas:**
   - `data_admissao` convertida para `DateType` (`yyyy-MM-dd`).
   - `data_desligamento` tratada para `NULL` quando vazia/nula, e convertida para `DateType` quando preenchida.
5. **Padronização de Status:** `initcap` para **`Ativo`** / **`Inativo`**.
6. **Idempotência:** Gravação Delta Lake com `.mode("overwrite")` e `.option("overwriteSchema", True)`.

---

### Tabela 2: Motivos de Atendimento (`vcredit.silver.tb_motivos`)

#### Diagnóstico da Base Bruta:
- `id_motivo` em formato texto.
- 3 registros com a coluna `categoria` em branco (`""`).
- Falta de acentuação em categorias (`'Cartao'`, `'Beneficios'`).
- Inconsistência de maiúsculas/minúsculas e acentuação em `criticidade` (`'alta'`, `'Media'`, `'baixa'`).

#### Tratamentos e Regras de Negócio:
1. **Deduplicação Defensiva:** Janela particionada por `id_motivo` ordenada pelo `ingestion_datetime` decrescente.
2. **Tipagem:** `id_motivo` convertido para `IntegerType()`.
3. **Imputação dos 5 Eixos Oficiais do Case:**
   - *Transferência de agência* $\rightarrow$ **`Cadastral`** (critério: alterações de dados do cliente, agência etc.)
   - *Contratação de cartão adicional* $\rightarrow$ **`Cartão`** (critério: operação, contratações e compras com cartão)
   - *Cancelamento de conta* $\rightarrow$ **`Financeiro`** (critério: relação contratual e encerramento com o banco)
4. **Correção Ortográfica:**
   - `'Cartao'` $\rightarrow$ **`Cartão`**
   - `'Beneficios'` $\rightarrow$ **`Benefícios`**
5. **Padronização da Criticidade:** Harmonizado estritamente para **`Alta`**, **`Média`** ou **`Baixa`**.
6. **Idempotência:** Gravação Delta Lake com `.mode("overwrite")` e `.option("overwriteSchema", True)`.

---

## 3. Notebook: `03_Silver_to_Gold_VCredit`

### Contexto e Objetivo
Transformação analítica da camada Silver para a modelagem dimensional (*Star Schema*) da camada Gold (`vcredit.gold`), preparando as tabelas dimensão para consumo no Power BI e cruzamento com a tabela fato de atendimentos.

---

### Tabela 1: Dimensão Atendente (`vcredit.gold.dim_atendente`)

#### Regras de Negócio e Enriquecimento:
- **Campos Selecionados:** `id_atendente`, `nome_atendente`, `nivel_atendimento`, `data_admissao`, `data_desligamento`, `status_atendente`.
- **Cálculo da Métrica `tempo_empresa` (em dias):**
  - Para colaboradores inativos (`data_desligamento IS NOT NULL`): `datediff(data_desligamento, data_admissao)`.
  - Para colaboradores ativos (`data_desligamento IS NULL`): `datediff(current_date(), data_admissao)`.
  - Tipagem convertida para `IntegerType()`.
- **Idempotência:** Gravação em formato Delta Lake com `.mode("overwrite")` e `.option("overwriteSchema", True)`.

---

### Tabela 2: Dimensão Motivos (`vcredit.gold.dim_motivo`)

#### Estrutura da Dimensão:
- **Campos Selecionados:** `id_motivo`, `nome_motivo`, `categoria`, `criticidade`.
- **Idempotência:** Gravação em formato Delta Lake com `.mode("overwrite")` e `.option("overwriteSchema", True)`.

---

## 4. Resumo da Nomenclatura das Camadas

- **Bronze:**
  - `vcredit.bronze.tb_atendentes`
  - `vcredit.bronze.tb_motivos`
- **Silver:**
  - `vcredit.silver.tb_atendentes`
  - `vcredit.silver.tb_motivos`
- **Gold:**
  - `vcredit.gold.dim_atendente`
  - `vcredit.gold.dim_motivo`
  - *(Integrada no Power BI com as demais dimensões `dim_cliente`, `dim_canal`, `dim_data` e a tabela `fato`)*.