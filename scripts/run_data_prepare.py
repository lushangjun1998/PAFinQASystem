import json
from typing import List, Dict
from langchain_classic.schema import Document
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
import config
from utils import load_json


class FieldKnowledgeBase:
    """字段知识库构建器"""

    def __init__(self):
        self.embeddings = HuggingFaceEmbeddings(
            model_name=config.EMBEDDING_MODEL,
            model_kwargs={'device': 'cpu'},
            encode_kwargs={'normalize_embeddings': True}
        )

    def prepare_documents(self) -> List[Document]:
        """准备用于向量检索的文档"""
        knowledge = load_json(config.FIELD_KNOWLEDGE_PATH)
        documents = []

        # 为每个字段创建文档
        for field_info in knowledge['fields']:
            # 构建文档内容
            content = f"表名: {field_info['table']}\n"
            content += f"字段名: {field_info['field']}\n"
            content += f"描述: {field_info['description']}\n"
            content += f"同义词: {', '.join(field_info['synonyms'])}\n"
            content += f"示例: {', '.join(str(e) for e in field_info['examples'])}"

            # 创建元数据
            metadata = {
                'table': field_info['table'],
                'field': field_info['field'],
                'description': field_info['description'],
                'synonyms': ','.join(field_info['synonyms'])
            }

            doc = Document(page_content=content, metadata=metadata)
            documents.append(doc)

            # 为每个同义词也创建一个文档（提高召回率）
            for synonym in field_info['synonyms']:
                synonym_content = f"表名: {field_info['table']}\n"
                synonym_content += f"字段名: {field_info['field']}\n"
                synonym_content += f"同义词: {synonym}\n"
                synonym_content += f"描述: {field_info['description']}"

                synonym_doc = Document(
                    page_content=synonym_content,
                    metadata={**metadata, 'synonym': synonym}
                )
                documents.append(synonym_doc)

        # 为表创建文档
        for table_info in knowledge['tables']:
            content = f"表名: {table_info['name']}\n"
            content += f"描述: {table_info['description']}\n"
            content += f"字段: {', '.join(table_info['fields'])}"

            metadata = {
                'table': table_info['name'],
                'is_table': True,
                'fields': ','.join(table_info['fields'])
            }

            doc = Document(page_content=content, metadata=metadata)
            documents.append(doc)

        return documents

    def build_vector_store(self):
        """构建FAISS向量库"""
        print(">>> 准备文档...")
        documents = self.prepare_documents()

        print(f">>> 构建向量库，共 {len(documents)} 个文档...")
        vector_store = FAISS.from_documents(documents, self.embeddings)

        print(f">>> 保存向量库到 {config.VECTOR_STORE_PATH}")
        vector_store.save_local(str(config.VECTOR_STORE_PATH))

        return vector_store


if __name__ == "__main__":
    builder = FieldKnowledgeBase()
    builder.build_vector_store()