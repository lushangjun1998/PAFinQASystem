import os
from pathlib import Path

# 路径配置
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data/fin_data.db"
VECTOR_STORE_PATH = BASE_DIR / "vector_store"
FIELD_KNOWLEDGE_PATH = BASE_DIR / "data/field_knowledge.json"

# 模型配置
EMBEDDING_MODEL = r"E:\PAFinQASystem\PAFinQASystem\model_dir\BAAI\bge-large-zh-v1.5"

# 公司内网大模型配置
LLM_MODEL_NAME = "qwen3-coder-next"
LLM_API_BASE = "https://dashscope.aliyuncs.com/compatible-mode/v1"
LLM_API_KEY = "sk-748acc62358b435c93edfd3c499266fd"
LLM_API_TYPE = "openai"  # API类型

# 检索配置
TOP_K_FIELDS = 10  # 召回候选字段数量
TOP_K_TABLES = 5   # 召回相关表数量

# SQL生成配置
MAX_SQL_RETRY = 1
SQL_TIMEOUT = 10

# 数据库配置
DB_TYPE = "sqlite"