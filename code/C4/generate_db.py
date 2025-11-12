import sqlite3
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# --- 1. 常量定义 ---
DB_NAME = "e_commerce.db"
NUM_CUSTOMERS = 200
NUM_PRODUCTS = 50
NUM_ORDERS = 500
NUM_ORDER_ITEMS = 1500
NUM_LOGISTICS = 480

# --- 2. 自定义真实感数据 ---
PRODUCT_CATEGORIES = {
    "手机": ["iPhone 15 Pro", "华为 Mate 60 Pro", "小米 14 Ultra", "三星 Galaxy S24", "OPPO Find X7"],
    "家电": ["海尔冰箱 BCD-500", "戴森 V15 吸尘器", "格力空调 KFR-35", "索尼电视 KD-65X90L", "美的微波炉 M1-L213B"],
    "服饰": ["Nike Air Force 1", "优衣库基础款T恤", "Adidas Stan Smith", "Levi's 501牛仔裤", "始祖鸟冲锋衣"],
    "饮品": ["农夫山泉 550ml", "可口可乐 330ml", "元气森林气泡水", "三得利乌龙茶", "星巴克拿铁"],
    "图书": ["《三体》", "《长安的荔枝》", "《额尔古纳河右岸》", "《代码大全》", "《原则》"]
}
CITIES = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '重庆', '南京', '天津', '西安', '长沙']

# 初始化Faker
fake = Faker('zh_CN')

# --- 3. 数据生成函数 (代码结构优化) ---

def generate_customers():
    data = []
    for i in range(NUM_CUSTOMERS):
        data.append({
            'customer_id': f'C{i+1:04d}',
            'customer_name': fake.name(),
            'city': random.choice(CITIES),
            'age': random.randint(18, 65),
            'gender': random.choice(['男', '女'])
        })
    return data

def generate_products():
    data = []
    for i in range(NUM_PRODUCTS):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        product_name = random.choice(PRODUCT_CATEGORIES[category])
        if random.random() > 0.5:
            product_name += f" {random.choice(['远峰蓝', '限定版', '256GB', '2024款'])}"
        
        data.append({
            'product_id': f'P{i+1:04d}',
            'product_name': product_name,
            'category': category,
            'price': round(random.uniform(10.0, 8000.0), 2)
        })
    return data

def generate_orders(customer_ids):
    data = []
    for i in range(NUM_ORDERS):
        order_date = fake.date_time_between(start_date='-2y', end_date='now').strftime('%Y-%m-%d')
        data.append({
            'order_id': f'O{i+1:04d}',
            'customer_id': random.choice(customer_ids),
            'order_date': order_date,
            'status': random.choice(['已完成', '已取消', '配送中'])
        })
    return data

def generate_order_items(orders_data, products_data):
    data = []
    # --- 逻辑优化：只为有效订单生成详情 ---
    valid_order_ids = [o['order_id'] for o in orders_data if o['status'] in ['已完成', '配送中']]
    product_ids_prices = {p['product_id']: p['price'] for p in products_data}
    
    for i in range(NUM_ORDER_ITEMS):
        product_id = random.choice(list(product_ids_prices.keys()))
        data.append({
            'order_item_id': i + 1,
            'order_id': random.choice(valid_order_ids),
            'product_id': product_id,
            'quantity': random.randint(1, 5),
            'unit_price': round(product_ids_prices[product_id] * random.uniform(0.9, 1.1), 2)
        })
    return data

def generate_logistics(orders_data):
    data = []
    shippable_order_ids = [o['order_id'] for o in orders_data if o['status'] in ['已完成', '配送中']]
    # --- 健壮性优化：确保k不大于样本数量 ---
    k = min(NUM_LOGISTICS, len(shippable_order_ids))
    selected_order_ids = random.sample(shippable_order_ids, k=k)

    for i, order_id in enumerate(selected_order_ids):
        order_date_str = next(o['order_date'] for o in orders_data if o['order_id'] == order_id)
        order_date = datetime.strptime(order_date_str, '%Y-%m-%d')
        shipping_date = order_date + timedelta(days=random.randint(0, 3))
        
        data.append({
            'logistics_id': i + 1,
            'order_id': order_id,
            'shipping_date': shipping_date.strftime('%Y-%m-%d'),
            'delivery_time_days': random.randint(1, 7),
            'shipping_company': random.choice(['顺丰快递', '中通快递', '圆通速递', '韵达快递'])
        })
    return data

def write_to_db(dataframes: dict, db_path: str):
    """将多个DataFrame写入SQLite数据库。"""
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"已删除旧的数据库文件: {db_path}")
        
    conn = sqlite3.connect(db_path)
    print(f"正在向数据库 '{db_path}' 写入数据...")
    for table_name, df in dataframes.items():
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    print("\n包含的表:")
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table in tables:
        print(f"- {table[0]}")
    
    conn.close()
    print(f"\n数据库 '{db_path}' 已成功创建并填充了模拟数据。")

# --- 4. 主执行流程 (代码结构优化) ---
def main():
    """主函数，执行所有步骤"""
    # 生成数据
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders([c['customer_id'] for c in customers])
    order_items = generate_order_items(orders, products)
    logistics = generate_logistics(orders)
    
    # 转换为DataFrame
    dataframes = {
        "customers": pd.DataFrame(customers),
        "products": pd.DataFrame(products),
        "orders": pd.DataFrame(orders),
        "order_items": pd.DataFrame(order_items),
        "logistics": pd.DataFrame(logistics)
    }
    
    # 写入数据库
    write_to_db(dataframes, DB_NAME)

if __name__ == "__main__":
    main()