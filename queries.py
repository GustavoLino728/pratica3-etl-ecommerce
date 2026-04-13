# queries.py
import duckdb

con = duckdb.connect("data/ecommerce.duckdb")

print("=== 1. Faturamento total por mês ===")
print(con.execute("""
    SELECT year_month,
           ROUND(SUM(total_item_value), 2) AS faturamento_total
    FROM analytical_sales
    GROUP BY year_month ORDER BY year_month
""").fetchdf().to_string())

print("\n=== 2. Faturamento por categoria ===")
print(con.execute("""
    SELECT category,
           ROUND(SUM(total_item_value), 2) AS faturamento
    FROM analytical_sales
    GROUP BY category ORDER BY faturamento DESC
""").fetchdf().to_string())

print("\n=== 3. Quantidade de pedidos por estado ===")
print(con.execute("""
    SELECT state, COUNT(DISTINCT order_id) AS total_pedidos
    FROM analytical_sales
    GROUP BY state ORDER BY total_pedidos DESC
""").fetchdf().to_string())

print("\n=== 4. Ticket médio por cliente (Top 20) ===")
print(con.execute("""
    SELECT customer_name,
           ROUND(SUM(total_item_value) / COUNT(DISTINCT order_id), 2) AS ticket_medio
    FROM analytical_sales
    GROUP BY customer_name ORDER BY ticket_medio DESC LIMIT 20
""").fetchdf().to_string())

print("\n=== 5. Top 10 produtos mais vendidos ===")
print(con.execute("""
    SELECT product_name, SUM(quantity) AS total_qty
    FROM analytical_sales
    GROUP BY product_name ORDER BY total_qty DESC LIMIT 10
""").fetchdf().to_string())

con.close()