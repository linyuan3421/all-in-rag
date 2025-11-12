import os
import json
from typing import List, Dict, Any
from langchain_deepseek import ChatDeepSeek

class SimpleSQLGenerator:
    """
    一个简化的SQL生成器，负责将自然语言问题转换为SQL查询。
    它利用LLM的能力，并结合从知识库检索到的上下文来生成更准确的SQL。
    核心功能包括：
    1. 根据上下文生成SQL。
    2. 在SQL执行失败时，根据错误信息进行修复。
    """

    def __init__(self, api_key: str = None):
        """
        初始化SQL生成器。
        
        Args:
            api_key (str, optional): DeepSeek API密钥. 如果未提供, 将从环境变量加载.
        """
        # 确保DEEPSEEK_API_KEY环境变量已设置
        api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY 环境变量未设置，请在 .env 文件或系统中配置。")
            
        self.llm = ChatDeepSeek(
            model="deepseek-chat",
            temperature=0,  # 使用较低的温度以确保SQL生成的确定性和稳定性
            api_key=api_key
        )

    def _build_context(self, knowledge_results: List[Dict[str, Any]]) -> str:
        """
        将从知识库检索到的信息构建成一个结构化的字符串，作为LLM的上下文。
        
        Args:
            knowledge_results (List[Dict[str, Any]]): 知识库的检索结果。
        
        Returns:
            str: 格式化后的上下文字符串。
        """
        context = ""
        
        # 按类型对知识进行分组
        ddl_info = [item['content'] for item in knowledge_results if item['type'] == 'ddl']
        desc_info = [item['content'] for item in knowledge_results if item['type'] == 'description']
        qsql_pairs = [item['content'] for item in knowledge_results if item['type'] == 'q-sql']
        
        # 按照 结构 -> 描述 -> 示例 的顺序构建上下文，这有助于LLM更好地理解
        if ddl_info:
            context += "=== 数据库表结构 (DDL) ===\n"
            context += "\n\n".join(ddl_info)
            context += "\n\n"
            
        if desc_info:
            context += "=== 表和字段的业务描述 ===\n"
            context += "\n".join(desc_info)
            context += "\n\n"
            
        if qsql_pairs:
            context += "=== 相关查询示例 (Q-SQL对) ===\n"
            context += "\n\n".join(qsql_pairs)
            context += "\n\n"
            
        return context.strip()

    def generate_sql(self, user_query: str, knowledge_results: List[Dict[str, Any]]) -> str:
        """
        根据用户问题和知识库上下文生成SQL语句。
        
        Args:
            user_query (str): 用户的自然语言查询。
            knowledge_results (List[Dict[str, Any]]): 知识库的检索结果。
        
        Returns:
            str: LLM生成的SQL查询语句。
        """
        context = self._build_context(knowledge_results)
        
        prompt = f"""你是一个SQL专家，你的任务是根据下面提供的数据库信息和用户问题，生成一句可以在SQLite数据库上执行的SQL查询语句。

        ### 数据库信息:
        {context}

        ### 用户问题:
        {user_query}

        ### 要求:
        1.  **只返回SQL语句**，不要包含任何额外的解释、注释或格式化（如 ```sql ... ```）。
        2.  确保SQL语法与 **SQLite** 兼容。
        3.  严格使用数据库信息中提供的表名和字段名。不要捏造不存在的字段。
        4.  如果需要进行多表连接（JOIN），请根据表结构中的主外键关系进行。
        5.  理解用户问题中的日期和数值，并正确地应用在WHERE子句中。

        SQL语句:
        """
        # print("\n--- 正在生成SQL... ---")
        # 打印一个简短的prompt预览，避免过长的输出
        # prompt_preview = (prompt[:300] + '...') if len(prompt) > 300 else prompt
        # print(f"Prompt预览: {prompt_preview}")
        
        response = self.llm.invoke(prompt)
        sql_query = response.content.strip()
        
        # 移除可能存在的markdown代码块标记
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        sql_query = sql_query.strip()

        print(f"生成的SQL: {sql_query}")
        return sql_query

    def fix_sql(self, user_query: str, original_sql: str, error_message: str, knowledge_results: List[Dict[str, Any]]) -> str:
        """
        当SQL执行失败时，尝试根据错误信息进行修复。
        
        Args:
            user_query (str): 原始的用户问题。
            original_sql (str): 执行失败的SQL语句。
            error_message (str): 数据库返回的错误信息。
            knowledge_results (List[Dict[str, Any]]): 原始的知识库检索结果。
            
        Returns:
            str: LLM生成的修复后的SQL查询语句。
        """
        context = self._build_context(knowledge_results)
        
        prompt = f"""你是一个SQL调试专家。你之前生成的SQL语句在执行时出错了，请根据错误信息和原始的数据库上下文来修正它。

        ### 数据库信息:
        {context}

        ### 用户问题:
        {user_query}

        ### 执行失败的SQL:
        ```sql
        {original_sql}
        ```

        ### 数据库返回的错误信息:
        {error_message}

        ### 要求:
        1.  **仔细分析错误原因**，并生成一句修正后的、正确的SQLite SQL查询语句。
        2.  **只返回修复后的SQL语句**，不要包含任何解释或道歉。

        修复后的SQL语句:
        """
        print("\n--- 正在修复SQL... ---")
        print(f"错误信息: {error_message}")
        
        response = self.llm.invoke(prompt)
        fixed_sql = response.content.strip()

        # 移除可能存在的markdown代码块标记
        if fixed_sql.startswith("```sql"):
            fixed_sql = fixed_sql[6:]
        if fixed_sql.endswith("```"):
            fixed_sql = fixed_sql[:-3]

        fixed_sql = fixed_sql.strip()
        
        print(f"修复后的SQL: {fixed_sql}")
        return fixed_sql
    
    def summarize_results(self, user_query: str, sql_results: Dict[str, Any]) -> str:
        """
        根据SQL查询结果，为用户问题生成一段自然语言的总结。(优化版)

        Args:
            user_query (str): 原始的用户问题。
            sql_results (Dict[str, Any]): 来自_execute_sql方法的查询结果字典。

        Returns:
            str: LLM生成的自然语言总结或预设的提示信息。
        """
        # 1. 检查是否为非查询操作
        if "columns" not in sql_results or "rows" not in sql_results:
            return "操作已执行，但没有数据可供总结。"

        # 2. 统一处理所有“未找到匹配数据”的情况 (核心优化)
        #    这覆盖了聚合函数返回null（rows是 [{'AVG(age)': None}]）
        #    和未匹配到任何行（rows是 []）两种情况
        has_meaningful_data = any(row for row in sql_results["rows"] if any(v is not None for v in row.values()))
        if not has_meaningful_data:
             return f"根据您的提问 “{user_query}”，数据库中没有找到符合条件的相关记录。"

        # 3. 如果有数据，则交给LLM进行总结
        results_str = json.dumps(sql_results, ensure_ascii=False, indent=2)
        
        prompt = f"""你是一个数据分析师，你的任务是根据用户提出的问题和数据库查询返回的JSON格式结果，用自然语言进行总结和解读。

        ### 用户原始问题:
        {user_query}

        ### 数据库查询结果 (JSON):
        ```json
        {results_str}
        ```

        ### 你的任务:
        1.  用清晰、简洁、友好的语言，直接回答用户的原始问题。
        2.  **不要**复述SQL语句或JSON结构。
        3.  如果结果是一个列表，可以总结关键的几项，例如“排名前三的是...”。
        4.  如果结果是一个数值，请清晰地呈现这个数值和它的含义。
        5.  你的回答应该是最终的、可以直接呈现给业务人员的结论。

        总结报告:
        """
        # print("\n--- 正在生成结果总结... ---")
        response = self.llm.invoke(prompt)
        summary = response.content.strip()
        # print(f"总结内容: {summary}")
        return summary