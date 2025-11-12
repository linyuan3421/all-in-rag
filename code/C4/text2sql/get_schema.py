import sqlite3
import json
import os

# --- 配置路径 ---
# 脚本期望从 a/b/c 运行，去访问 a/b 下的文件
# 我们当前在 .../code/C4/text2sql/
# 数据库在 .../code/C4/
DB_PATH = os.path.join("..", "e_commerce.db")
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ddl.json")

def get_db_schema(db_path):
    # 1. 健壮性检查：确保数据库文件存在
    if not os.path.exists(db_path):
        print(f"错误：数据库文件在路径 '{os.path.abspath(db_path)}' 未找到。")
        print("请确保您已经运行了 generate_db.py 并且 e_commerce.db 文件位于 code/C4/ 目录下。")
        return None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    schema_info = []
    print(f"在数据库中找到 {len(tables)} 个表: {tables}")
    for table_name in tables:
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_statement = cursor.fetchone()[0]
        schema_info.append({
            "content": create_statement,
            "type": "ddl"
        })
        
    conn.close()
    return schema_info

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    schema_data = get_db_schema(DB_PATH)
    
    # 2. 确保只有在成功获取数据后才写入文件
    if schema_data:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, ensure_ascii=False, indent=4)
        print(f"\n数据库结构已成功提取并保存到 '{OUTPUT_FILE}'")
        # 打印部分内容以供验证
        print("--- ddl.json 文件内容预览 ---")
        print(json.dumps(schema_data[0], ensure_ascii=False, indent=2))
        print("...")
    else:
        print("\n未能生成 ddl.json 文件，因为无法从数据库获取结构信息。")