import sqlite3
from typing import Dict, Any, List, Tuple

# 从当前目录下的文件中导入我们之前写好的类
from .knowledge_base import SimpleKnowledgeBase
from .sql_generator import SimpleSQLGenerator

class SimpleText2SQLAgent:
    """
    一个简单的Text2SQL代理，协调知识库、SQL生成器和数据库执行。
    """
    def __init__(self, db_path: str, api_key: str = None):
        """
        初始化代理。

        Args:
            db_path (str): SQLite数据库文件的路径。
            api_key (str, optional): DeepSeek API密钥. 如果未提供, 将从环境变量加载.
        """
        self.db_path = db_path
        self.connection = None
        self.knowledge_base = SimpleKnowledgeBase()
        self.sql_generator = SimpleSQLGenerator(api_key)

        # --- 可配置参数 ---
        self.max_retry_count = 3  # 最大重试次数
        self.top_k_retrieval = 8  # 知识库检索数量 (可以适当增加以提供更丰富上下文)
        self.max_result_rows = 100 # SQL查询结果返回的最大行数，防止数据过多

    def connect_database(self):
        """连接到SQLite数据库。"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            print(f"--- 成功连接到数据库: {self.db_path} ---")
        except Exception as e:
            print(f"--- 数据库连接失败: {e} ---")
            raise  # 重新抛出异常，让调用者知道失败了

    def _execute_sql(self, sql: str) -> Tuple[bool, Any]:
        """
        在连接的数据库上安全地执行SQL语句。

        Args:
            sql (str): 要执行的SQL语句。

        Returns:
            Tuple[bool, Any]: 一个元组，第一个元素表示是否成功，
                             第二个元素是结果（成功时）或错误信息（失败时）。
        """
        try:
            cursor = self.connection.cursor()
            
            # 安全检查：对SELECT语句自动添加LIMIT，防止返回过多数据
            if sql.strip().upper().startswith('SELECT') and 'LIMIT' not in sql.upper():
                sql_with_limit = f"{sql.rstrip(';')} LIMIT {self.max_result_rows};"
                print(f"自动添加LIMIT: {sql_with_limit}")
                cursor.execute(sql_with_limit)
            else:
                cursor.execute(sql)

            # 如果是查询语句，获取结果
            if sql.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                # 将结果格式化为字典列表
                results = [dict(zip(columns, row)) for row in rows]
                
                return True, {
                    "columns": columns,
                    "rows": results,
                    "count": len(results)
                }
            else:
                # 对于非查询语句 (INSERT, UPDATE, DELETE), 提交事务
                self.connection.commit()
                return True, f"操作成功，影响行数: {cursor.rowcount}"
        
        except Exception as e:
            return False, str(e) # 返回错误信息字符串
        
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()

    def run(self, user_question: str) -> Dict[str, Any]:
        """
        执行完整的Text2SQL查询流程。

        Args:
            user_question (str): 用户的自然语言问题。

        Returns:
            Dict[str, Any]: 包含执行结果的字典。
        """
        if not self.connection:
            return {"success": False, "error": "数据库未连接"}

        print(f"\n{'='*20} 开始处理新查询 {'='*20}")
        print(f"用户问题: {user_question}")

        # 1. 知识库检索
        knowledge_results = self.knowledge_base.search(user_question, self.top_k_retrieval)

        # 2. 生成初始SQL
        sql = self.sql_generator.generate_sql(user_question, knowledge_results)

        # 3. 执行与重试循环
        retry_count = 0
        while retry_count < self.max_retry_count:
            print(f"\n--- 正在执行SQL (第 {retry_count + 1} 次尝试) ---")
            success, result = self._execute_sql(sql)

            if success:
                print("--- SQL执行成功！ ---")
                summary = self.sql_generator.summarize_results(user_question, result)
                
                return {
                    "success": True,
                    "question": user_question,
                    "sql": sql,
                    "results": result,
                    "summary": summary
                }
            else: # 如果执行失败
                error_message = result
                print(f"--- SQL执行失败: {error_message} ---")
                
                # 如果还有重试机会，则尝试修复
                if retry_count < self.max_retry_count - 1:
                    sql = self.sql_generator.fix_sql(user_question, sql, error_message, knowledge_results)
                
                retry_count += 1
        
        # 如果所有重试都失败了
        print(f"--- 超过最大重试次数，查询失败 ---")
        return {
            "success": False,
            "question": user_question,
            "error": f"SQL执行失败并在{self.max_retry_count}次重试后仍然失败。最后一次错误: {error_message}",
            "last_sql_attempt": sql
        }

    def close(self):
        """关闭数据库连接。"""
        if self.connection:
            self.connection.close()
            print("\n--- 数据库连接已关闭 ---")