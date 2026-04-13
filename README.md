# Prática 3 - Pipeline ETL Batch com Dados Fake

## Descrição
Pipeline ETL em lote simulando processamento de dados de e-commerce com bases sintéticas.

## Estrutura
```
.
├── generate_fake_data.py   # Gera dados fake com Faker
├── etl_pipeline.py         # Pipeline ETL (Extract, Transform, Load)
├── pratica3_etl_ecommerce.ipynb  # Notebook com execução e evidencias
└── data/
    ├── raw/              # CSVs gerados (customers, products, orders, order_items)
    └── ecommerce.duckdb  # Banco de dados DuckDB
```

## Como executar
```bash
pip install faker duckdb pandas
python generate_fake_data.py
python etl_pipeline.py
```

## Tabelas criadas no DuckDB
- `raw_customers`, `raw_products`, `raw_orders`, `raw_order_items` (brutas)
- `treated_orders`, `treated_order_items` (tratadas)
- `analytical_sales` (analítica final)

## Consultas Analíticas
1. Faturamento total por mês
2. Faturamento por categoria
3. Quantidade de pedidos por estado
4. Ticket médio por cliente
5. Top 10 produtos mais vendidos
