import sqlite3
import json

DB_NAME = "../../e_commerce.db" # 注意路径，我们现在在text2sql文件夹里
OUTPUT_FILE = "data/ddl.json"

def get_db_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取所有表名
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    schema_info = []
    for table_name in tables:
        # sqlite_master中的sql字段存储了创建表的语句
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_statement = cursor.fetchone()[0]
        schema_info.append({
            "content": create_statement,
            "type": "ddl"
        })
        
    conn.close()
    return schema_info

if __name__ == "__main__":
    import os
    os.makedirs("data", exist_ok=True)
    
    schema_data = get_db_schema(DB_NAME)
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(schema_data, f, ensure_ascii=False, indent=4)
        
    print(f"数据库结构已成功提取并保存到 '{OUTPUT_FILE}'")