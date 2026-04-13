
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

fake = Faker("pt_BR")
random.seed(42)
Faker.seed(42)

STATES = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
          "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
CATEGORIES = ["Eletronicos","Roupas","Calcados","Alimentos","Livros",
              "Esportes","Casa e Jardim","Brinquedos","Beleza","Automotivo"]
ORDER_STATUSES = ["pending","processing","shipped","delivered","cancelled"]

os.makedirs("data/raw", exist_ok=True)

def generate_customers(n=3000):
    data = [{"customer_id":i,"customer_name":fake.name(),"city":fake.city(),
             "state":random.choice(STATES),
             "signup_date":fake.date_between(start_date="-5y",end_date="today").isoformat()}
            for i in range(1,n+1)]
    df = pd.DataFrame(data)
    df.loc[random.sample(range(n),int(n*0.01)),"city"] = None
    df.to_csv("data/raw/customers.csv",index=False)
    return df

def generate_products(n=300):
    data = [{"product_id":i,"product_name":fake.catch_phrase(),
             "category":random.choice(CATEGORIES),
             "price":round(random.uniform(10.0,2000.0),2)}
            for i in range(1,n+1)]
    df = pd.DataFrame(data)
    df.to_csv("data/raw/products.csv",index=False)
    return df

def generate_orders(customers_df, n=10000):
    ids = customers_df["customer_id"].tolist()
    base = datetime(2023,1,1)
    data = [{"order_id":i,"customer_id":random.choice(ids),
             "order_date":(base+timedelta(days=random.randint(0,730))).date().isoformat(),
             "status":random.choice(ORDER_STATUSES)} for i in range(1,n+1)]
    df = pd.DataFrame(data)
    df.loc[random.sample(range(n),int(n*0.005)),"status"] = None
    df.to_csv("data/raw/orders.csv",index=False)
    return df

def generate_order_items(orders_df, products_df, n=20000):
    oids = orders_df["order_id"].tolist()
    pids = products_df["product_id"].tolist()
    prices = dict(zip(products_df["product_id"],products_df["price"]))
    data = [{"order_item_id":i,"order_id":random.choice(oids),"product_id":(pid:=random.choice(pids)),
             "quantity":random.randint(1,10),"unit_price":prices[pid]} for i in range(1,n+1)]
    df = pd.DataFrame(data)
    df.to_csv("data/raw/order_items.csv",index=False)
    return df

if __name__ == "__main__":
    c = generate_customers(3000)
    p = generate_products(300)
    o = generate_orders(c, 10000)
    generate_order_items(o, p, 20000)
    logger.info("Dados fake gerados com sucesso!")
