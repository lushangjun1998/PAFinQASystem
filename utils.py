import json
import sqlite3
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd


def load_json(file_path):
    """加载JSON文件"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_date_from_question(question: str) -> Optional[str]:
    """从问题中提取日期信息"""
    # 匹配常见日期格式
    patterns = [
        r'(\d{4})年(\d{1,2})月',  # 2021年03月
        r'(\d{4})-(\d{1,2})',  # 2021-03
        r'(\d{4})年',  # 2021年
    ]

    for pattern in patterns:
        match = re.search(pattern, question)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                year, month = groups
                return f"{year}{int(month):02d}"
            elif len(groups) == 1:
                return groups[0]
    return None


def extract_number_from_question(question: str) -> Optional[float]:
    """从问题中提取数字"""
    match = re.search(r'(\d+(?:\.\d+)?)', question)
    if match:
        return float(match.group(1))
    return None


def get_table_schema(db_path: str, table_name: str) -> str:
    """获取表结构的文本描述"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    schema = f"表名: {table_name}\n字段:\n"
    for col in columns:
        schema += f"  - {col[1]} ({col[2]})\n"

    conn.close()
    return schema


def execute_sql(db_path: str, sql: str) -> List[Dict[str, Any]]:
    """执行SQL并返回结果"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(sql)
        results = [dict(row) for row in cursor.fetchall()]
        return results
    except Exception as e:
        raise e
    finally:
        conn.close()


def format_results(results: List[Dict[str, Any]]) -> str:
    """格式化查询结果"""
    if not results:
        return "无结果"

    df = pd.DataFrame(results)
    return df.to_string(index=False)