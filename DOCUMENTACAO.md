# Documentação do Pipeline: Bronze e Silver (V-Credit)

Este documento reúne todo o racional, as regras de negócio aplicadas e o guia passo a passo da documentação utilizada nos notebooks de ingestão e tratamento do Case V-Credit.

---

## 1. Notebook: `01_Ingestao_Bronze_VCredit`

### Contexto e Objetivo
Configurar o ambiente analítico no Databricks e realizar a ingestão inicial dos dados brutos das bases cadastrais/dimensionais da operação:
- `base_atendentes.csv`
- `base_motivos.csv`

### Passos Realizados:
1. **Setup do Ambiente:**
   - Criação com cláusula defensiva (`IF NOT EXISTS`) do catálogo (`vcredit_analytics`), dos schemas da arquitetura medalhão (`bronze`, `silver`, `gold`) e do volume Unity Catalog (`/Volumes/vcredit_analytics/bronze/inputs`).
2. **Ingestão Raw:**
   - Leitura com `inferSchema=False` e `header=True` para preservar o formato original e não corromper dados antes da hora.
   - Adição do metadado de auditoria `ingestion_datetime` (`current_timestamp()`).
3. **Garantia de Idempotência:**
   - Escrita em formato **Delta Lake** com `.mode("overwrite")` e `overwriteSchema="true"` em:
     - `bronze.tb_atendentes`
     - `bronze.tb_motivos`
   - O job pode ser reexecutado múltiplas vezes sem inchar a base ou duplicar linhas.

---

## 2. Notebook: `02_Bronze_to_Silver_VCredit`

### Contexto e Objetivo
Leitura da camada Bronze para aplicação de limpeza, tipagem estrita, saneamento de dados faltantes, padronizações e enriquecimento de negócio.

---

### Tabela 1: Atendentes (`silver.tb_atendentes`)

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
6. **Idempotência:** Gravação Delta Lake com `.mode("overwrite")`.

---

### Tabela 2: Motivos de Atendimento (`silver.tb_motivos`)

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
6. **Idempotência:** Gravação Delta Lake com `.mode("overwrite")`.

---

## 3. Resumo da Nomenclatura das Camadas

- **Bronze:**
  - `vcredit_analytics.bronze.tb_atendentes`
  - `vcredit_analytics.bronze.tb_motivos`
- **Silver:**
  - `vcredit_analytics.silver.tb_atendentes`
  - `vcredit_analytics.silver.tb_motivos`
- **Gold (Próxima etapa):**
  - Onde serão geradas as dimensões com prefixo `dim_` (`dim_atendente`, `dim_motivo`) e fatos (`fct_chamados`) para os dashboards analíticos.
