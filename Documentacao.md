# Documentação: Bronze e Silver — Pesquisa de Satisfação, Custos e Canais

Este documento descreve a ingestão e o tratamento das três tabelas sob responsabilidade desta parte do projeto V-Credit: **pesquisa de satisfação**, **custos** e **canais**.
---

## 1. Setup do Ambiente e Ingestão Bronze

### Contexto e padrão adotado

A ingestão é realizada no Databricks, a partir dos arquivos CSV armazenados no Volume Landing.

- **Catálogo:** `vcredit`
- **Schemas:** `vcredit.bronze`, `vcredit.silver` e `vcredit.gold`
- **Volume de entrada:** `/Volumes/vcredit/bronze/Landing/`

### Tabelas ingeridas

| Arquivo de origem | Tabela Bronze | Quantidade esperada de registros |
|---|---|---:|
| `pesquisa_satisfacao.csv` | `vcredit.bronze.pesquisa_satisfacao` | 81.021 |
| `custos.csv` | `vcredit.bronze.custos` | 131.069 |
| `canais.csv` | `vcredit.bronze.canais` | 4 |

### Etapas realizadas na Bronze

1. **Configuração do ambiente:** criação defensiva do catálogo `vcredit`, dos schemas `bronze`, `silver` e `gold` e do Volume `Landing`, utilizando `IF NOT EXISTS`.
2. **Leitura dos arquivos CSV:** os arquivos são lidos com cabeçalho, codificação UTF-8 e `inferSchema=False`. Assim, os valores são preservados inicialmente como texto, evitando conversões automáticas prematuras.
3. **Tratamento de falhas de leitura:** o modo `FAILFAST` interrompe a leitura quando é encontrado um erro de processamento do arquivo.
4. **Metadados de auditoria:** são adicionadas as colunas `_arquivo_origem`, com o caminho do arquivo de origem, e `_data_ingestao`, com o instante da ingestão.
5. **Persistência:** cada DataFrame é gravado como tabela Delta no schema Bronze, com `mode("overwrite")` e `overwriteSchema=true`.
6. **Idempotência operacional:** a reexecução substitui o conteúdo da tabela pelo conteúdo atual do arquivo, evitando o acúmulo de cópias causado por inserções repetidas. A operação é de substituição, não de histórico incremental.
7. **Validação da carga:** o notebook apresenta a contagem de registros, o schema e uma amostra das tabelas. Também compara as quantidades obtidas com as quantidades esperadas definidas no código.

### Função de ingestão

O processo utiliza uma função comum para as três bases. Ela recebe o nome do arquivo e o nome da tabela de destino, executa a leitura, adiciona os metadados, grava a tabela e retorna um resumo com arquivo, tabela, quantidade de registros e status.

---

## 2. Notebook `Silver.ipynb`

### Contexto e objetivo

A camada Silver aplica tratamentos específicos para cada conjunto de dados. O objetivo é reduzir inconsistências de formatação, converter valores para tipos apropriados e criar indicadores que permitam identificar registros válidos ou que precisam de atenção. As tabelas resultantes são gravadas em Delta Lake no schema `vcredit.silver`.

As transformações são executadas novamente sobre a Bronze e gravadas com `mode("overwrite")` e `overwriteSchema=true`.

---

### Tabela 1: Pesquisa de Satisfação

#### Tratamentos realizados

1. **Limpeza dos identificadores:** `id_pesquisa` e `id_chamado` são convertidos para texto, têm espaços nas extremidades removidos e recebem `NULL` quando o valor está vazio.
2. **Conversão da nota:** `nota_atendimento` é convertida para inteiro com `try_cast`. Antes da conversão, são tratados espaços e o texto literal `NULL`. Valores que não puderem ser convertidos ficam nulos, em vez de interromper todo o processamento.
3. **Validação da nota:** é criada a coluna `nota_valida`, marcada como verdadeira quando `nota_atendimento` está entre 1 e 5, inclusive. Notas fora desse intervalo e notas nulas são identificadas como inválidas.
4. **Identificação de possíveis duplicidades:** a coluna `id_pesquisa_duplicado` sinaliza registros cujo `id_pesquisa` não nulo aparece mais de uma vez.
5. **Remoção de duplicatas exatas:** `dropDuplicates()` remove linhas integralmente duplicadas. Essa operação não garante, por si só, que exista apenas uma linha para cada `id_pesquisa`; a coluna indicadora permite investigar esse caso.
6. **Persistência:** a tabela tratada é salva em `vcredit.silver.pesquisa_satisfacao`, substituindo a versão anterior.

#### Principais colunas utilizadas ou geradas

| Coluna | Finalidade |
|---|---|
| `id_pesquisa` | Identificador da pesquisa, tratado como texto |
| `id_chamado` | Identificador do chamado relacionado, tratado como texto |
| `nota_atendimento` | Nota convertida para inteiro |
| `nota_valida` | Indicador de nota dentro do intervalo de 1 a 5 |
| `id_pesquisa_duplicado` | Indicador de identificador de pesquisa repetido |

As demais colunas que existirem no CSV são mantidas, salvo alterações explicitamente descritas no notebook.

---

### Tabela 2: Custos

#### Diagnóstico e tratamentos realizados

1. **Limpeza dos identificadores:** `id_custo` e `id_chamado` são convertidos para texto e têm espaços nas extremidades removidos.
2. **Preservação do valor de entrada:** é criada a coluna `custo_original`, que guarda a representação textual original de `custo` para permitir conferência após a normalização.
3. **Remoção de elementos monetários:** o valor é tratado sem diferenciar maiúsculas e minúsculas, removendo `R$`, as palavras `real`/`reais`, espaços e caracteres que não sejam dígitos, vírgula, ponto ou sinal negativo.
4. **Padronização dos separadores decimais:** o código identifica a posição de vírgulas e pontos para tratar formatos como `1.234,56` e `1,234.56`, removendo o separador de milhar quando necessário e normalizando o separador decimal para ponto.
5. **Conversão numérica:** o valor normalizado é convertido para `DECIMAL(12,2)` na coluna `custo`.
6. **Validação do custo:** é criada a coluna `custo_valido`, verdadeira quando o valor foi convertido corretamente e é maior ou igual a zero. Valores negativos ou não conversíveis ficam sinalizados como inválidos.
7. **Remoção de duplicatas exatas:** `dropDuplicates()` elimina linhas integralmente duplicadas.
8. **Persistência:** os dados tratados são gravados em `vcredit.silver.custos`, substituindo a versão anterior.

#### Principais colunas utilizadas ou geradas

| Coluna | Finalidade |
|---|---|
| `id_custo` | Identificador do registro de custo, tratado como texto |
| `id_chamado` | Identificador do chamado relacionado, tratado como texto |
| `custo_original` | Representação textual original do custo |
| `custo` | Valor normalizado em `DECIMAL(12,2)` |
| `custo_valido` | Indicador de conversão válida e custo não negativo |

A coluna auxiliar `_custo_normalizado` é removida antes da gravação final. As demais colunas de origem são mantidas, exceto quando explicitamente removidas no notebook.

---

### Tabela 3: Canais

#### Tratamentos realizados

1. **Limpeza do identificador:** `id_canal` é convertido para texto e tem espaços nas extremidades removidos.
2. **Padronização textual:** `nome_canal`, `tipo_canal` e `status_canal` são convertidos para texto, têm espaços nas extremidades removidos e são padronizados para letras minúsculas.
3. **Validação dos campos obrigatórios:** é criada a coluna `canal_valido`. Ela é verdadeira quando `id_canal`, `nome_canal`, `tipo_canal` e `status_canal` não são nulos nem vazios.
4. **Remoção de duplicatas exatas:** `dropDuplicates()` remove registros com todas as colunas iguais.
5. **Persistência:** a tabela é gravada em `vcredit.silver.canais`, substituindo a versão anterior.

#### Principais colunas utilizadas ou geradas

| Coluna | Finalidade |
|---|---|
| `id_canal` | Identificador do canal, tratado como texto |
| `nome_canal` | Nome do canal padronizado em minúsculas |
| `tipo_canal` | Tipo do canal padronizado em minúsculas |
| `status_canal` | Status do canal padronizado em minúsculas |
| `canal_valido` | Indicador de preenchimento dos quatro campos obrigatórios |

A validação cria um indicador, mas não remove automaticamente os registros inválidos. Eles permanecem disponíveis na Silver para inspeção e para decisões posteriores de qualidade.

---

## 3. Relatório de Qualidade

O notebook contém uma função de resumo que calcula o total de registros e a quantidade de chaves nulas ou vazias. Quando é fornecida uma coluna de validação, também calcula quantos registros não passam nessa validação.

No código atual, o relatório é montado para:

- `pesquisa_satisfacao`, usando `id_pesquisa` como chave e `nota_valida` como indicador de validade;
- `custos`, usando `id_custo` como chave e `custo_valido` como indicador de validade.

**Observação:** embora a tabela `canais` possua o indicador `canal_valido`, ela não está incluída na lista que alimenta esse relatório de qualidade. Portanto, para avaliar os canais, é necessário consultar essa coluna separadamente ou acrescentar a tabela ao relatório.

O relatório não corrige automaticamente os registros sinalizados: ele fornece indicadores para apoiar a análise e o tratamento de problemas de qualidade.

---

## 4. Considerações para as próximas camadas

- **Pesquisa de satisfação:** utilizar `id_chamado` para relacionar as respostas aos chamados. Antes de agregá-las em uma tabela fato, verificar se há mais de uma pesquisa por chamado e decidir a regra de agregação adequada.
- **Custos:** utilizar `id_chamado` para relacionar os custos aos chamados. Como a base possui registros de custo, não se deve presumir que exista apenas um registro por chamado; caso necessário, agregar os custos antes de fazer o join com a tabela de chamados para evitar multiplicação de linhas.
- **Canais:** a dimensão de canais pode ser construída a partir de `vcredit.silver.canais`, aproveitando `id_canal`, `nome_canal`, `tipo_canal` e `status_canal`. Antes de usá-la no modelo dimensional, conferir se `id_canal` é único e se não há registros com `canal_valido = false`.
- **Chaves e tipos:** os identificadores de pesquisa, custo, chamado e canal são tratados como texto neste notebook. As camadas seguintes devem manter tipos compatíveis nas chaves utilizadas nos relacionamentos.
- **Validade versus exclusão:** `nota_valida`, `custo_valido` e `canal_valido` são indicadores de qualidade; sua criação não significa que os registros inválidos foram removidos.

---

## 5. Resumo da nomenclatura das tabelas

### Bronze

- `vcredit.bronze.pesquisa_satisfacao`
- `vcredit.bronze.custos`
- `vcredit.bronze.canais`

### Silver

- `vcredit.silver.pesquisa_satisfacao`
- `vcredit.silver.custos`
- `vcredit.silver.canais`

### Metadados adicionados na Bronze

- `_arquivo_origem`
- `_data_ingestao`

### Indicadores de qualidade adicionados na Silver

- `nota_valida`
- `id_pesquisa_duplicado`
- `custo_valido`
- `canal_valido`

---