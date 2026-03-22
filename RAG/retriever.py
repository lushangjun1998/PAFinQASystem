from typing import List, Dict, Any, Tuple
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_classic.retrievers import EnsembleRetriever
from langchain_classic.retrievers.bm25 import BM25Retriever
from langchain_classic.schema import Document
import config
from utils import load_json


class HybridFieldRetriever:
    """混合检索器：向量检索 + BM25"""

    def __init__(self):
        # 初始化向量检索
        self.embeddings = HuggingFaceEmbeddings(
            model_name=config.EMBEDDING_MODEL,
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': True}
        )

        # 加载向量库
        self.vector_store = FAISS.load_local(
            str(config.VECTOR_STORE_PATH),
            self.embeddings,
            allow_dangerous_deserialization=True
        )

        # 准备BM25检索器
        knowledge = load_json(config.FIELD_KNOWLEDGE_PATH)
        bm25_docs = []
        for field_info in knowledge['fields']:
            content = (
                f"字段名:{field_info['field']}, " * 2 +
                f"所属表名: {field_info['table']}, "
                f"同义词: {','.join(field_info['synonyms'])}, " +
                f"数据示例: {','.join(str(e) for e in field_info['examples'])};\n"
            )
            doc = Document(
                page_content=content,
                metadata={'table': field_info['table'], 'field': field_info['field']}
            )
            bm25_docs.append(doc)

        self.bm25_retriever = BM25Retriever.from_documents(bm25_docs)
        self.bm25_retriever.k = config.TOP_K_FIELDS

        # 创建混合检索器
        self.ensemble_retriever = EnsembleRetriever(
            retrievers=[self.vector_store.as_retriever(search_kwargs={"k": config.TOP_K_FIELDS}),
                        self.bm25_retriever],
            weights=[0.7, 0.3]
        )

    def retrieve_fields(self, question: str) -> Tuple[List[Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        检索相关字段和表
        返回: (匹配内容, 字段列表, 表列表)
        """
        # 混合检索
        docs = self.ensemble_retriever.invoke(question)

        fields = []
        pages = []
        seen_fields = set()
        seen_tables = set()

        for doc in docs:
            metadata = doc.metadata
            page_content = doc.page_content

            # 如果是表文档
            if metadata.get('is_table'):
                table_name = metadata['table']
                if table_name not in seen_tables:
                    # tables.append({
                    #     'name': table_name,
                    #     'fields': metadata['fields'].split(',') if metadata.get('fields') else []
                    # })
                    seen_tables.add(table_name)
            else:
                # 字段文档
                field_key = f"{metadata['table']}.{metadata['field']}"
                if field_key not in seen_fields:
                    pages.append(page_content)
                    fields.append({
                        'table': metadata['table'],
                        'field': metadata['field'],
                        'description': metadata.get('description', '')
                    })
                    seen_fields.add(field_key)

        fields = fields[:config.TOP_K_FIELDS]
        pages = pages[:config.TOP_K_FIELDS]
        tables = list(set([field['table'] for field in fields] + list(seen_tables)))
        return pages, fields, tables

    def get_relevant_context(self, question: str) -> str:
        """获取检索到的上下文信息，用于LLM提示"""
        pages, fields, tables = self.retrieve_fields(question)

        context = "【相关字段信息】\n"
        for page in pages:
            context += page
            # context += f"- 表名: {field['table']}, 字段名: {field['field']}, 描述: {field['description']}\n"

        context += "\n【相关数据库表结构】\n"
        knowledge = load_json(config.FIELD_KNOWLEDGE_PATH)
        for table_info in knowledge['tables']:
            if table_info['name'] in tables:
                context += f"- 表名: {table_info['name']}, 包含字段: {', '.join(table_info['fields'])};\n"

        return context