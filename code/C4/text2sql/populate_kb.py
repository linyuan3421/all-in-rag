from knowledge_base import SimpleKnowledgeBase

# 确保你的Milvus服务正在运行！
kb = SimpleKnowledgeBase()

# 从JSON文件加载数据
kb.load_data_from_json("data/ddl.json")
kb.load_data_from_json("data/descriptions.json")
kb.load_data_from_json("data/q_sql_pairs.json")

print("\n知识库填充完成！")

# 测试一下搜索功能
test_query = "哪个品类的销售额最高？"
search_results = kb.search(test_query)

print(f"\n--- 测试搜索结果 ---")
for result in search_results:
    print(f"类型: {result['type']}, 相似度: {result['score']:.4f}")
    print(f"内容: {result['content']}")
    print("-" * 20)