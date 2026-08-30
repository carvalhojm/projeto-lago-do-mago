# 🧙 Lago do Mago

## Data Lake & Lakehouse na AWS + Databricks

Projeto de engenharia de dados desenvolvido a partir do curso do [Teo Me Why](https://github.com/TeoMeWhy/lago-mago), com implementação e adaptações realizadas durante o acompanhamento do curso.

### Arquitetura

```text
                         ┌─────────────────┐
                         │ SISTEMA DE      │
                         │ ORIGEM          │
                         └────────┬────────┘
                                  │
                                 CDC
                                  ▼
                               ┌──────┐
                               │ RAW  │
                               └──┬───┘
                                  │
                           Auto Loader
                                  │
                              Streaming
                                  ▼
                            ┌───────────┐
                            │  BRONZE   │
                            └─────┬─────┘
                                  │
                                 CDF
                                  │
                              Streaming
                                  ▼
                            ┌───────────┐
                            │  SILVER   │
                            └─────┬─────┘
                                  │
                         regras analíticas
                                  │
                                  ▼
                            ┌───────────┐
                            │   GOLD    │
                            └─────┬─────┘
                                  │
                      ┌───────────┴───────────┐
                      │                       │
                   DAILY                   MONTHLY
                      │                       │
                      └───────────┬───────────┘
                                  │
                             CUBOS / SQL
                                  │
                                  ▼
                         DASH EXECUTIVO
                                  │
                                  ▼
                              NEGÓCIO
```

### Relatório Executivo

O dashboard final conta com visualizações de:

- Transações MAU
- MAU (Monthly Active Users)
- Pontuação MAU
- Pontos Acumulados
- Qtde. Produtos por Ano
- % Clientes x Produto
- % Transações x Produto

### Tecnologias

`AWS` · `Databricks` · `Delta Lake` · `Apache Spark` · `Python` · `SQL` · `GitHub Actions`

### Objetivo

Demonstrar, na prática, a construção de um pipeline de dados moderno, incremental e automatizado, desde a ingestão até a disponibilização de informações para análise e tomada de decisão.
