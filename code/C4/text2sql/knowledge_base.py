import json
import os
from typing import List, Dict, Any
from pymilvus import MilvusClient, FieldSchema, CollectionSchema, DataType
from pymilvus.model.hybrid import BGEM3EmbeddingFunction

class SimpleKnowledgeBase:
    """
    一个简化的知识库，使用Milvus存储和检索与Text2SQL相关的知识。
    知识类型包括：
    1. DDL: 数据库表的创建语句。
    2. Description: 表和字段的业务描述。
    3. Q-SQL: "问题-SQL"示例对。
    """
    def __init__(self, collection_name: str = "text2sql_ecommerce_kb"):
        # --- 从环境变量安全地加载Zilliz Cloud配置 ---
        ZILLIZ_CLOUD_URI = os.getenv("ZILLIZ_CLOUD_URI")
        ZILLIZ_CLOUD_USER = os.getenv("ZILLIZ_CLOUD_USER")
        ZILLIZ_CLOUD_PASSWORD = os.getenv("ZILLIZ_CLOUD_PASSWORD")
        
        # 2. 检查变量是否存在，如果不存在则给出清晰的错误提示
        if not all([ZILLIZ_CLOUD_URI, ZILLIZ_CLOUD_USER, ZILLIZ_CLOUD_PASSWORD]):
            raise ValueError("Zilliz Cloud的环境变量未设置完整 (ZILLIZ_CLOUD_URI, ZILLIZ_CLOUD_USER, ZILLIZ_CLOUD_PASSWORD)")

        self.collection_name = collection_name
        
        # 3. 使用从环境中读取的值来创建客户端
        self.client = MilvusClient(
            uri=ZILLIZ_CLOUD_URI,
            user=ZILLIZ_CLOUD_USER,
            password=ZILLIZ_CLOUD_PASSWORD
        )
        
        # 使用BGE-M3同时生成密集和稀疏向量，但本基础版只使用密集向量
        self.embedding_function = BGEM3EmbeddingFunction(use_fp16=False, device="cpu")
        self.dense_dim = self.embedding_function.dim["dense"]
        
        self._setup_collection()

    def _setup_collection(self):
        """如果集合不存在，则创建它。"""
        if self.client.has_collection(self.collection_name):
            print(f"集合 '{self.collection_name}' 已存在，跳过创建。")
        else:
            print(f"集合 '{self.collection_name}' 不存在，开始创建...")
            fields = [
                FieldSchema(name="pk", dtype=DataType.VARCHAR, is_primary=True, auto_id=True, max_length=100),
                FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=4096),
                FieldSchema(name="type", dtype=DataType.VARCHAR, max_length=32),
                FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=self.dense_dim)
            ]
            schema = CollectionSchema(fields, description="Text2SQL电商知识库")
            self.client.create_collection(self.collection_name, schema=schema)
            print("集合创建成功。正在创建索引...")
            
            index_params = self.client.prepare_index_params()
            index_params.add_index(field_name="dense_vector", index_type="AUTOINDEX", metric_type="IP")
            self.client.create_index(self.collection_name, index_params)
            print("索引创建成功。")

            # ⬇️⬇️⬇️ 加上这一行代码 ⬇️⬇️⬇️
        print(f"正在加载集合 '{self.collection_name}' 到内存中...")
        self.client.load_collection(self.collection_name)
        print("集合加载成功。")

    def load_data_from_json(self, file_path: str):
        """从JSON文件加载数据并插入知识库。"""
        print(f"正在从 '{file_path}' 加载数据...")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"错误：文件 '{file_path}' 未找到。")
            return
            
        contents = [item['content'] for item in data]
        types = [item['type'] for item in data]
        
        if not contents:
            print("文件中没有可加载的内容。")
            return
            
        print(f"正在为 {len(contents)} 条内容生成向量...")
        embeddings = self.embedding_function(contents)
        dense_vectors = embeddings['dense']
        
        entities = []
        for i in range(len(contents)):
            entities.append({
                "content": contents[i],
                "type": types[i],
                "dense_vector": dense_vectors[i]
            })
            
        print(f"正在向集合 '{self.collection_name}' 插入 {len(entities)} 条实体...")
        self.client.insert(self.collection_name, data=entities)
        print("数据插入成功。")
        
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """根据查询，从知识库中搜索最相关的信息。"""
        print(f"\n正在知识库中搜索与 '{query}' 相关的信息...")
        query_embeddings = self.embedding_function([query])
        
        search_results = self.client.search(
            collection_name=self.collection_name,
            data=query_embeddings["dense"],
            limit=top_k,
            output_fields=["content", "type"]
        )
        
        # 格式化输出
        results_list = []
        for hits in search_results:
            for hit in hits:
                results_list.append({
                    "score": hit['distance'],
                    "content": hit['entity']['content'],
                    "type": hit['entity']['type']
                })
        print(f"检索到 {len(results_list)} 条相关信息。")
        return results_list