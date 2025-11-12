import sqlite3
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# 初始化Faker用于生成模拟数据
fake = Faker('zh_CN')

# --- 1. 定义数据量 ---
NUM_CUSTOMERS = 200
NUM_PRODUCTS = 50
NUM_ORDERS = 500
NUM_ORDER_ITEMS = 1500
NUM_LOGISTICS = 480 # 假设部分订单未发货或状态异常

# --- 2. 生成模拟数据 ---

# 顾客数据
customers_data = []
for i in range(NUM_CUSTOMERS):
    customers_data.append({
        'customer_id': f'C{i+1:04d}',
        'customer_name': fake.name(),
        'city': fake.city(),
        'age': random.randint(18, 65),
        'gender': random.choice(['男', '女'])
    })

# 商品数据
products_data = []
categories = ['手机', '家电', '服饰', '饮品', '汽车', '图书', '美妆']
for i in range(NUM_PRODUCTS):
    product_name = fake.word() + random.choice(['手机', '电视', 'T恤', '可乐', 'SUV', '小说', '口红'])
    products_data.append({
        'product_id': f'P{i+1:04d}',
        'product_name': product_name,
        'category': random.choice(categories),
        'price': round(random.uniform(10.0, 5000.0), 2)
    })

# 订单数据
orders_data = []
customer_ids = [c['customer_id'] for c in customers_data]
for i in range(NUM_ORDERS):
    order_date = fake.date_time_between(start_date='-2y', end_date='now').strftime('%Y-%m-%d')
    orders_data.append({
        'order_id': f'O{i+1:04d}',
        'customer_id': random.choice(customer_ids),
        'order_date': order_date,
        'status': random.choice(['已完成', '已取消', '配送中'])
    })

# 订单详情数据
order_items_data = []
order_ids = [o['order_id'] for o in orders_data]
product_ids_prices = {p['product_id']: p['price'] for p in products_data}
for i in range(NUM_ORDER_ITEMS):
    product_id = random.choice(list(product_ids_prices.keys()))
    order_items_data.append({
        'order_item_id': i + 1,
        'order_id': random.choice(order_ids),
        'product_id': product_id,
        'quantity': random.randint(1, 5),
        'unit_price': product_ids_prices[product_id] * random.uniform(0.9, 1.1) # 价格略有浮动
    })
    
# 物流数据
logistics_data = []
completed_order_ids = [o['order_id'] for o in orders_data if o['status'] in ['已完成', '配送中']]
shuffled_order_ids = random.sample(completed_order_ids, k=min(NUM_LOGISTICS, len(completed_order_ids)))

for i, order_id in enumerate(shuffled_order_ids):
    order_date_str = next(o['order_date'] for o in orders_data if o['order_id'] == order_id)
    order_date = datetime.strptime(order_date_str, '%Y-%m-%d')
    shipping_date = order_date + timedelta(days=random.randint(0, 3))
    logistics_data.append({
        'logistics_id': i + 1,
        'order_id': order_id,
        'shipping_date': shipping_date.strftime('%Y-%m-%d'),
        'delivery_time_days': random.randint(1, 7),
        'shipping_company': random.choice(['顺丰快递', '中通快递', '圆通速递', '韵达快递'])
    })


# --- 3. 转换为DataFrame ---
customers_df = pd.DataFrame(customers_data)
products_df = pd.DataFrame(products_data)
orders_df = pd.DataFrame(orders_data)
order_items_df = pd.DataFrame(order_items_data)
logistics_df = pd.DataFrame(logistics_data)

# --- 4. 写入SQLite数据库 ---
DB_NAME = "e_commerce.db"
conn = sqlite3.connect(DB_NAME)

customers_df.to_sql('customers', conn, if_exists='replace', index=False)
products_df.to_sql('products', conn, if_exists='replace', index=False)
orders_df.to_sql('orders', conn, if_exists='replace', index=False)
order_items_df.to_sql('order_items', conn, if_exists='replace', index=False)
logistics_df.to_sql('logistics', conn, if_exists='replace', index=False)

print(f"数据库 '{DB_NAME}' 已成功创建并填充了模拟数据。")
print("\n包含的表:")
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    print(f"- {table[0]}")

conn.close()