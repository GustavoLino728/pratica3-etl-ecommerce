import pandas as pd
import duckdb
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("etl_pipeline")
DB_PATH = "data/ecommerce.duckdb"

def extract():
    logger.info("=== ETAPA 1: EXTRACAO ===")
    customers   = pd.read_csv("data/raw/customers.csv")
    products    = pd.read_csv("data/raw/products.csv")
    orders      = pd.read_csv("data/raw/orders.csv")
    order_items = pd.read_csv("data/raw/order_items.csv")
    logger.info(f"customers: {len(customers)} registros")
    logger.info(f"products: {len(products)} registros")
    logger.info(f"orders: {len(orders)} registros")
    logger.info(f"order_items: {len(order_items)} registros")
    return customers, products, orders, order_items

def transform(customers, products, orders, order_items):
    logger.info("=== ETAPA 2: TRANSFORMACAO ===")

    # Ajuste de tipos
    customers["signup_date"] = pd.to_datetime(customers["signup_date"])
    orders["order_date"]     = pd.to_datetime(orders["order_date"])
    products["price"]        = products["price"].astype(float)
    order_items["unit_price"] = order_items["unit_price"].astype(float)
    order_items["quantity"]   = order_items["quantity"].astype(int)
    logger.info("Tipos ajustados")

    # Tratamento de nulos
    nulos_city   = customers["city"].isna().sum()
    nulos_status = orders["status"].isna().sum()
    customers["city"]  = customers["city"].fillna("Desconhecida")
    orders["status"]   = orders["status"].fillna("pending")
    logger.info(f"Nulos tratados -> city: {nulos_city} | status: {nulos_status}")

    # Calculo total por item
    order_items["total_item_value"] = order_items["quantity"] * order_items["unit_price"]
    logger.info("Coluna total_item_value calculada")

    # Juncao entre tabelas
    df = order_items.merge(orders, on="order_id", how="left")
    df = df.merge(products[["product_id","product_name","category"]], on="product_id", how="left")
    df = df.merge(customers[["customer_id","customer_name","city","state"]], on="customer_id", how="left")
    df["year_month"] = df["order_date"].dt.to_period("M").astype(str)
    logger.info(f"Base consolidada: {len(df)} registros, {len(df.columns)} colunas")

    return customers, products, orders, order_items, df

def load(customers, products, orders, order_items, df_joined):
    logger.info("=== ETAPA 3: CARGA ===")
    con = duckdb.connect(DB_PATH)

    for name, df in [("raw_customers", customers), ("raw_products", products),
                     ("raw_orders", orders), ("raw_order_items", order_items)]:
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.register(f"_{name}", df)
        con.execute(f"CREATE TABLE {name} AS SELECT * FROM _{name}")
        logger.info(f"Tabela bruta criada: {name} ({len(df)} registros)")

    con.execute("DROP TABLE IF EXISTS treated_orders")
    con.register("_orders_reg", orders)
    con.execute("""
        CREATE TABLE treated_orders AS
        SELECT order_id, customer_id, order_date::DATE AS order_date, status
        FROM _orders_reg
    """)
    logger.info("Tabela tratada criada: treated_orders")

    con.execute("DROP TABLE IF EXISTS treated_order_items")
    con.register("_items_reg", order_items)
    con.execute("""
        CREATE TABLE treated_order_items AS
        SELECT order_item_id, order_id, product_id, quantity, unit_price,
               ROUND(quantity * unit_price, 2) AS total_item_value
        FROM _items_reg
    """)
    logger.info("Tabela tratada criada: treated_order_items")

    con.execute("DROP TABLE IF EXISTS analytical_sales")
    con.register("_analytical", df_joined)
    con.execute("CREATE TABLE analytical_sales AS SELECT * FROM _analytical")
    logger.info("Tabela analitica criada: analytical_sales")

    tabelas = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
    logger.info(f"Tabelas no banco: {tabelas}")
    con.close()
    return DB_PATH

if __name__ == "__main__":
    start = datetime.now()
    logger.info(f"Pipeline iniciado em: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    c, p, o, oi = extract()
    c, p, o, oi, df = transform(c, p, o, oi)
    load(c, p, o, oi, df)
    duracao = datetime.now() - start
    logger.info(f"Pipeline ETL concluido em {duracao}")