import sqlite3
import json
import os

# --- 配置路径 ---
# 脚本期望从 .../code/C4/text2sql/ 运行, 去访问 .../code/C4/ 下的文件
# DB_PATH 指向上一级目录的数据库文件
DB_PATH = os.path.join("..", "e_commerce.db") 

# OUTPUT_DIR 指向当前目录下的 'data' 文件夹
OUTPUT_DIR = "data" 
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "ddl.json")

def get_db_schema(db_path):
    # 1. 健壮性检查: 确保数据库文件存在
    if not os.path.exists(db_path):
        # os.path.abspath会显示绝对路径，方便调试
        print(f"错误: 数据库文件在路径 '{os.path.abspath(db_path)}' 未找到。")
        print(f"请确保您已经运行了 generate_db.py 并且 e_commerce.db 文件位于 code/C4/ 目录下。")
        return None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    if not tables:
        print("错误: 数据库中没有找到任何表。")
        conn.close()
        return None
        
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

def main():
    """主函数，执行所有步骤"""
    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    schema_data = get_db_schema(DB_PATH)
    
    if schema_data:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, ensure_ascii=False, indent=4)
        print(f"数据库结构已成功提取并保存到 '{os.path.abspath(OUTPUT_FILE)}'")

if __name__ == "__main__":
    main()