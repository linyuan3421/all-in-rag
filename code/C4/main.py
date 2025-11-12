import os
import json
from dotenv import load_dotenv

# 导入我们自己编写的Agent
from text2sql.text2sql_agent import SimpleText2SQLAgent

def pretty_print_json(data):
    """美观地打印JSON（字典）数据"""
    print(json.dumps(data, indent=4, ensure_ascii=False))

def main():
    # --- 1. 加载环境变量 ---
    # 这会加载项目根目录下的 .env 文件中的变量
    load_dotenv() 
    
    # 检查DeepSeek API密钥是否存在
    if not os.getenv("DEEPSEEK_API_KEY"):
        raise ValueError("请在.env文件中设置DEEPSEEK_API_KEY")

    # --- 2. 初始化Agent ---
    # 指定我们之前创建的SQLite数据库的路径
    db_path = "e_commerce.db"
    agent = SimpleText2SQLAgent(db_path=db_path)

    # --- 3. 连接数据库 ---
    agent.connect_database()

    # --- 4. 定义测试问题 ---
    test_questions = [
        "列出价格最贵的前5个商品。",
        "“上海”地区的顾客总共消费了多少金额？",
        "哪个品类的平均物流耗时最长？",
        "有没有叫“李强”的顾客？如果有，他下了多少订单？",
        "统计每个品类（category）的商品数量（product_count），并按数量降序排列。",
        "查询所有订单的ID、顾客姓名和商品名称。" 
    ]
    
    try:
        # --- 5. 循环执行查询 ---
        for question in test_questions:
            result = agent.run(user_question=question)
            
            print("\n--- 最终结果 ---")

            print(f"\n用户问题: {result.get('question')}")
            
            if result.get('success'):
                # 优先展示总结
                print(f"\n最终总结:\n{result.get('summary')}\n")
                
                # 在折叠的细节中提供技术信息
                print("--- [message_start] ---")
                print(f"\n生成的SQL:\n{result.get('sql')}")
                print("\n原始查询结果 (JSON):")
                pretty_print_json(result.get('results'))
                print("--- [message_end] ---")

            else:
                # 如果失败，清晰地展示错误信息
                print(f"\n❌ 执行失败:\n{result.get('error')}")
                print(f"最后的SQL: {result.get('last_sql_attempt')}")

            print("="*75 + "\n")
            
    except Exception as e:
        print(f"程序运行出错: {e}")
    finally:
        # --- 6. 关闭数据库连接 ---
        agent.close()

if __name__ == "__main__":
    main()