from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
import config
from utils import get_table_schema, execute_sql
from langchain_ollama import ChatOllama
import re

class SQLGenerator:
    """SQL生成器"""

    def __init__(self, retriever):
        # 初始化检索器
        self.retriever = retriever  # HybridFieldRetriever()

        # 本地大模型
        self.llm = ChatOllama(
            model="qwen3:8b",  # 本地Ollama部署的模型名称
            base_url="http://localhost:11434",  # Ollama默认端口
            temperature=0.7
        )
        # 云端API
        # self.llm = ChatOpenAI(
        #     model=config.LLM_MODEL_NAME,
        #     openai_api_base=config.LLM_API_BASE,
        #     openai_api_key=config.LLM_API_KEY
        #     # temperature=0.1,  # 低温度保证确定性输出
        #     # max_tokens=1024,
        #     # top_p=0.9,
        #     # frequency_penalty=0,
        #     # presence_penalty=0,
        #     # timeout=30,  # 30秒超时
        #     # max_retries=2,
        #     # model_kwargs={
        #     #     "stop": ["</s>", "```"],  # 设置停止标记
        #     # }
        # )

        # SQL生成提示模板（优化后的prompt）
        self.sql_prompt = PromptTemplate(
            input_variables=["question", "context", "db_schema"],
            template="""你是一个金融数据领域的SQL专家。请根据用户问题、相关字段信息和相关数据库表结构，生成正确的SQLite查询语句。

【用户问题】
{question}

【相关字段信息】
{context}

【所有数据库表信息】
{db_schema}

【重要要求】
1. 只输出SQL语句，不要任何解释、不要添加任何额外文字;
2. 注意：昨收盘(元)、今开盘(元)、最高价(元)、最低价(元)、收盘价(元)、成交量(股)、成交金额(元)、所属国家(地区)这几个是带有括号的字段，生成sql时要用双引号包裹字段名，例如"收盘价(元)"，以免sql运行错误;
2. 使用SQLite语法;
3. 如果问题涉及"数量"、"多少"等词，通常需要使用COUNT(*);
4. 如果问题涉及"最大"、"最小"等词，通常需要使用MAX/MIN和ORDER BY;
5. 如果问题涉及"差额"，需要计算两个字段的差值;
6. 如果问题涉及日期，注意日期格式转换（SQLite中可以使用strftime或直接比较）;
7. 如果需要多表关联，使用JOIN on关联字段;
8. 确保SQL正确且高效;

直接输出SQL语句：/no_think """.strip()
        )

        # 构建处理链
        self.chain = (
                {"question": RunnablePassthrough(),
                 "context": lambda x: self._get_context(x),
                 "db_schema": lambda x: self._get_db_schema()
                }
                | self.sql_prompt
                | self.llm
                | StrOutputParser()
                | self._clean_sql
        )

    def _get_context(self, question):
        """获取检索到的上下文"""
        context = self.retriever.get_relevant_context(question)
        return context

    def _get_db_schema(self):
        """获取数据库完整schema"""
        tables = [
            "基金基本信息表, 字段: 基金代码, 基金简称, 基金全称, 管理人, 托管人, 基金类型, 成立日期, 到期日期, 管理费率, 托管费率",
            "基金股票持仓明细表, 字段: 基金代码, 基金简称, 持仓日期, 股票代码, 股票名称, 数量, 市值, 市值占基金资产净值比, 第N大重仓股, 所在证券市场, 所属国家(地区), 报告类型",
            "基金债券持仓明细表, 字段: 基金代码, 基金简称, 持仓日期, 债券类型, 债券名称, 持债数量, 持债市值, 持债市值占基金资产净值比, 第N大重仓股, 所在证券市场, 所属国家(地区), 报告类型",
            "基金可转债持仓明细表, 字段: 基金代码, 基金简称, 持仓日期, 对应股票代码, 债券名称, 数量, 市值, 市值占基金资产净值比, 第N大重仓股, 所在证券市场, 所属国家(地区), 报告类型",
            "基金日行情表, 字段: 基金代码, 交易日期, 单位净值, 复权单位净值, 累计单位净值, 资产净值",
            "A股票日行情表, 字段: 股票代码, 交易日, 昨收盘(元), 今开盘(元), 最高价(元), 最低价(元), 收盘价(元), 成交量(股), 成交金额(元)",
            "港股票日行情表, 字段: 股票代码, 交易日, 昨收盘(元), 今开盘(元), 最高价(元), 最低价(元), 收盘价(元), 成交量(股), 成交金额(元)",
            "A股公司行业划分表, 字段: 股票代码, 交易日期, 行业划分标准, 一级行业名称, 二级行业名称",
            "基金规模变动表, 字段: 基金代码, 基金简称, 公告日期, 截止日期, 报告期期初基金总份额, 报告期基金总申购份额, 报告期基金总赎回份额, 报告期期末基金总份额, 定期报告所属年度, 报告类型",
            "基金份额持有人结构表, 字段: 基金代码, 基金简称, 公告日期, 截止日期, 机构投资者持有的基金份额, 机构投资者持有的基金份额占总份额比例, 个人投资者持有的基金份额, 个人投资者持有的基金份额占总份额比例, 定期报告所属年度, 报告类型"
        ]
        schema = ';\n'.join(tables)
        # schema = ""
        # for table in tables:
        #     schema += get_table_schema(config.DB_PATH, table) + "\n"

        return schema

    def _clean_sql(self, sql: str) -> str:
        """清理生成的SQL"""
        sql = sql.strip()
        sql = re.sub(r'<think>.*?</think>', '', sql, flags=re.DOTALL).strip()

        # 移除可能的markdown代码块标记
        if sql.startswith("```sql"):
            sql = sql[6:]
        elif sql.startswith("```"):
            sql = sql[3:]

        if sql.endswith("```"):
            sql = sql[:-3]

        # 移除可能的"SQL:"前缀
        if sql.lower().startswith("sql:"):
            sql = sql[4:]

        # 移除开头和结尾的空白
        sql = sql.strip()

        # 确保SQL以分号结尾（SQLite不强制，但习惯）
        if sql and not sql.endswith(";"):
            sql += ";"

        return sql

    def generate_sql(self, question: str) -> str:
        """生成SQL"""
        try:
            # 使用chain直接生成
            sql = self.chain.invoke(question)
            return sql
        except Exception as e:
            print(f"SQL生成错误: {e}")
            # 返回一个基本的错误查询
            return "SELECT 'SQL生成失败' as error;"

    def validate_sql(self, sql: str) -> bool:
        """简单验证SQL（可选）"""
        # 可以添加基本的SQL语法检查
        forbidden_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
        sql_upper = sql.upper()

        for keyword in forbidden_keywords:
            if keyword in sql_upper:
                print(f"SQL包含禁止的关键词: {keyword}")
                return False

        # 检查基本结构
        if not sql_upper.startswith('SELECT'):
            print("SQL不是SELECT查询")
            return False

        return True

    def generate_with_retry(self, question: str) -> str:
        """带重试的SQL生成"""
        for attempt in range(config.MAX_SQL_RETRY):
            try:
                sql = self.generate_sql(question)

                # 简单验证
                if not sql or len(sql) < 10:
                    print(f"SQL太短，重试 {attempt + 1}")
                    continue

                if not self.validate_sql(sql):
                    print(f"SQL验证失败，重试 {attempt + 1}")
                    continue

                # 尝试执行
                test_results = execute_sql(config.DB_PATH, sql)

                return sql

            except Exception as e:
                print(f"SQL生成尝试 {attempt + 1} 失败: {e}")
                continue

        return "SELECT '无法生成有效SQL' as error;"