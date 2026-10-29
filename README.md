# V-Credit --- Pipeline de Dados e Análises

Projeto de engenharia e análise de dados desenvolvido com Azure
Databricks, PySpark, Delta Lake e Power BI. O pipeline organiza dados de
atendimento em camadas, aplica regras de qualidade, constrói um modelo
dimensional e disponibiliza tabelas analíticas para relatórios.

## Sumário

-   [Objetivos](#objetivos)
-   [Tecnologias](#tecnologias)
-   [Arquitetura](#arquitetura)
-   [Estrutura do projeto](#estrutura-do-projeto)
-   [Camadas de dados](#camadas-de-dados)
-   [Modelo dimensional](#modelo-dimensional)
-   [Análises Gold](#análises-gold)
-   [Fluxo de clientes](#fluxo-de-clientes)
-   [Exportação CSV](#exportação-csv)
-   [Execução e agendamento](#execução-e-agendamento)

## Objetivos

-   Ingerir e organizar dados do V-Credit.
-   Padronizar tipos, identificadores, datas, valores monetários e
    categorias.
-   Sinalizar problemas de qualidade de dados.
-   Construir dimensões e uma tabela fato para análise multidimensional.
-   Gerar indicadores de desempenho, satisfação, custos, canais e
    demanda.
-   Analisar a sequência de canais percorrida por cada cliente.
-   Disponibilizar os dados no Power BI e exportá-los para CSV.
-   Automatizar as etapas por meio de Databricks Jobs.

## Tecnologias

  -----------------------------------------------------------------------
  Tecnologia                          Finalidade
  ----------------------------------- -----------------------------------
  Databricks                    Ambiente de processamento e
                                      execução dos notebooks

  Apache Spark / PySpark              Transformações distribuídas

  Delta Lake                          Persistência das tabelas

  Unity Catalog                       Catálogo, schemas e volumes, quando
                                      habilitado

  Power BI                            Relatórios e dashboards

  SQL e YAML                          Consultas e configuração do Job

  CSV                                 Arquivos de entrada e exportação
  -----------------------------------------------------------------------

## Arquitetura

``` text
Arquivos CSV / Volume Landing
            |
            v
    Landing_to_Bronze
            |
            v
       Bronze
            |
            v
    Bronze_to_Silver
            |
            v
        Silver
            |
            v
     Silver_to_Gold
            |
            v
         Gold
            |
            v
       Analises_Gold
            |
            v
   Tabelas analíticas / views
            |
            |
            v
       Exportação CSV
            |
            v
          Power BI
```

## Estrutura do projeto

A organização observada no workspace é semelhante a:

``` text
vcredit/
├── resources/
├── src/
│   ├── Landing_to_Bronze
│   ├── Bronze_to_Silver
│   ├── Silver_to_Gold
│   ├── Analises_Gold
│   └── Exportacao_Dados   
├── tabelas/                 
├── databricks.yml
└── README.md
```

Os caminhos precisam corresponder aos nomes reais dos notebooks no
workspace. O diretório `tabelas/` visível no explorador do projeto não é
necessariamente um caminho gravável pelo Spark no cluster remoto.

## Camadas de dados

O catálogo principal é `vcredit`, organizado em schemas como
`vcredit.bronze`, `vcredit.silver` e `vcredit.gold`.

### Bronze

A Bronze preserva os dados de entrada com transformações mínimas. As
práticas documentadas incluem:

-   Leitura de CSV com cabeçalho, UTF-8 e `inferSchema=False`.
-   Modo `FAILFAST` para interromper a leitura diante de erros.
-   Inclusão de `_arquivo_origem` e `_data_ingestao`.
-   Persistência em Delta Lake.
-   Conferência de schema e contagem de registros.

As cargas descritas usam `overwrite`: reexecutar substitui os dados
anteriores pelos dados atuais, não mantendo automaticamente um histórico
incremental.

### Silver

A Silver aplica limpeza, tipagem, padronização e indicadores de
qualidade. Tabelas documentadas incluem:

-   `vcredit.silver.pesquisa_satisfacao`
-   `vcredit.silver.custos`
-   `vcredit.silver.canais`
-   `vcredit.silver.tb_atendentes`
-   `vcredit.silver.tb_motivos`
-   `vcredit.silver.tb_chamados_hora`

Os nomes efetivamente disponíveis devem ser confirmados no catálogo.

Tratamentos de pesquisa de satisfação: - Limpeza de `id_pesquisa` e
`id_chamado`. - Conversão de `nota_atendimento` para inteiro. -
`nota_valida` para notas de 1 a 5. - `id_pesquisa_duplicado` para
sinalizar identificadores repetidos. - Remoção de duplicatas exatas.

Tratamentos de custos: - Limpeza dos identificadores. - Preservação do
valor original em `custo_original`. - Normalização de formatos
monetários e separadores decimais. - Conversão para `DECIMAL(12,2)`. -
`custo_valido` para indicar valores convertidos e não negativos. -
Remoção de duplicatas exatas.

Tratamentos de canais: - Padronização de `id_canal`, `nome_canal`,
`tipo_canal` e `status_canal`. - `canal_valido` para sinalizar campos
obrigatórios ausentes. - Remoção de duplicatas exatas.

Tratamentos de atendentes e motivos incluem deduplicação defensiva por
`ingestion_datetime`, conversão de identificadores, padronização de
níveis, datas, status, categorias e criticidade.

**Nota:** indicadores de validade sinalizam registros a investigar; não
significam que todos os registros inválidos foram excluídos.

### Gold

A Gold fornece dados preparados para análise, com dimensões, tabela fato
e tabelas analíticas Delta.

## Modelo dimensional

Tabelas centrais documentadas:

  -----------------------------------------------------------------------
  Tabela                              Finalidade
  ----------------------------------- -----------------------------------
  `vcredit.gold.dim_atendente`        Dados cadastrais e experiência dos
                                      atendentes

  `vcredit.gold.dim_canal`            Identificação e classificação dos
                                      canais

  `vcredit.gold.dim_cliente`          Atributos de clientes

  `vcredit.gold.dim_data`             Calendário para filtros e
                                      agrupamentos temporais

  `vcredit.gold.dim_motivo`           Classificação dos motivos

  `vcredit.gold.fact_chamados`        Chamados e métricas associadas
  -----------------------------------------------------------------------

### Dimensão Data

`dim_data` possui um registro por dia no intervalo gerado pelo notebook.
Os campos documentados incluem:

-   `id_tempo`: chave no formato `yyyyMMdd`, por exemplo `20260525`.
-   `data`, `dia`, `mes`, `ano`.
-   `dia_semana` e `nome_dia_semana`.
-   `trimestre` e `fim_de_semana`.

A fato relaciona-se à dimensão por `id_data` → `id_tempo`. No Power BI,
use `dim_data[data]` para filtros temporais e ordene `nome_dia_semana`
por `dia_semana`.

### Dimensão Atendente

Inclui identificador, nome, nível, datas de admissão/desligamento e
status. `tempo_empresa` é calculado em dias, usando a data de
desligamento para colaboradores desligados e a data atual para
colaboradores ativos.

### Dimensão Canal

Inclui `id_canal`, `nome_canal`, `tipo_canal` e `status_canal`. Para o
fluxo, os canais são classificados em URA, Chatbot, Atendimento Inicial
e Atendimento Especializado.

### Fato de chamados

A estrutura documentada inclui `id_chamado`, `id_cliente`,
`id_atendente`, `id_motivo`, `id_canal`, `id_data`, `resolvido`,
`custo`, `nota_atendimento`, `tempo_espera_seg` e
`tempo_atendimento_seg`. Confirme o schema real e a cobertura das
métricas antes de usá-las em indicadores.

## Análises Gold

O notebook `Analises_Gold` combina a fato e as dimensões e gera tabelas
analíticas. Nomes identificados nos notebooks incluem:

  -----------------------------------------------------------------------
  Tabela analítica                    Objetivo
  ----------------------------------- -----------------------------------
  `vw_chamados_detalhado`             Base detalhada enriquecida pelas
                                      dimensões

  `vw_desempenho_canal`               Volume e indicadores por canal

  `vw_desempenho_atendente`           Indicadores por nível/faixa de
                                      experiência

  `vw_demanda_dia_semana`             Demanda por dia da semana

  `vw_autosservico_faixa_etaria`      Volume e resolução por canal/faixa
                                      etária

  `vw_qualidade_dados`                Indicadores agregados de qualidade

  `vw_fluxo_cliente_canais`           Eventos e transições por cliente,
                                      se o notebook de fluxo tiver sido
                                      executado
  -----------------------------------------------------------------------

Para conferir os objetos atualmente existentes:

``` sql
SHOW TABLES IN vcredit.gold;
```

Regras documentadas: - **CSAT:** notas de 1 a 5; notas 4 e 5 são
consideradas satisfeitas na análise documentada. - **Custos:** valores
ausentes devem ser tratados como falta de cobertura, não automaticamente
como custo zero. - **Experiência:** pode ser calculada na data do
chamado para a análise de desempenho. - **Autosserviço:** a análise
considera valores de canal como Autosserviço, Digital, URA, Chatbot, App
ou Site, conforme o código. - **Qualidade:** indicadores auxiliam a
investigação e não substituem decisões de tratamento.

## Fluxo de clientes

A análise do fluxo usa os horários de início e fim, o cliente e o canal
para ordenar os eventos. A sequência esperada é:

``` text
URA ───────────────────────┐
                           ├─> Atendimento Inicial
Chatbot ───────────────────┘
                                  |
                                  v
                       Atendimento Especializado
```

O resultado deve permitir consultar cliente, sessão, chamado, ordem,
data, canal, etapa, status da transição, duração e intervalo entre
atendimentos.

Exemplo de consulta de um cliente:

``` sql
SELECT
    id_cliente,
    numero_sessao,
    ordem_na_sessao,
    id_chamado,
    data_evento,
    nome_canal,
    etapa_canal,
    status_fluxo,
    gap_anterior_minutos
FROM vcredit.gold.vw_fluxo_cliente_canais
WHERE id_cliente = 'ID_DO_CLIENTE'
ORDER BY numero_sessao, ordem_na_sessao;
```

Substitua `ID_DO_CLIENTE` pelo identificador desejado. Valide amostras
reais antes de tratar o resultado como indicador oficial.

## Exportação CSV

O notebook `Exportacao_Dados` pode exportar dimensões, fato, views e
resumos. Exemplo de organização
desejada:

``` text
tabelas/
├── dim_atendente.csv
├── dim_canal.csv
├── dim_cliente.csv
├── dim_data.csv
├── dim_motivo.csv
├── fact_chamados.csv
├── vw_chamados_detalhado.csv
└── ...
```

O diretório do Workspace não é automaticamente um caminho de sistema de
arquivos gravável pelo Spark. Em execução remota, prefira um caminho
permitido, como um Volume Unity Catalog
(`/Volumes/<catalog>/<schema>/<volume>/...`) ou armazenamento em nuvem
autorizado.

## Execução e agendamento

Ordem recomendada:

1.  `Landing_to_Bronze`
2.  `Bronze_to_Silver`
3.  `Silver_to_Gold`
4.  `Analises_Gold`
5.  `Tratamento_e_validacao_tempos`
6.  `Exportacao_Dados`

O YAML do Job deve declarar cada tarefa separadamente e definir
dependências corretas. Exemplo:

``` yaml
- task_key: analises_gold_views
  depends_on:
    - task_key: silver_to_gold
  notebook_task:
    notebook_path: ../src/Analises_Gold.ipynb
    source: WORKSPACE

- task_key: exportacao_dados
  depends_on:
    - task_key: analises_gold_views
  notebook_task:
    notebook_path: ../src/Exportacao_Dados.ipynb
    source: WORKSPACE
```

Exemplo de execução a cada quatro horas:

``` yaml
trigger:
  pause_status: UNPAUSED
  periodic:
    interval: 4
    unit: HOURS
```

------------------------------------------------------------------------