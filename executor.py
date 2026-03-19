import sqlite3
from typing import List, Dict, Any
import pandas as pd
import config
from utils import execute_sql, format_results


class SQLExecutor:
    """SQL执行器"""

    def __init__(self):
        self.db_path = config.DB_PATH

    def execute(self, sql: str) -> Dict[str, Any]:
        """执行SQL并返回结果"""
        try:
            results = execute_sql(self.db_path, sql)

            return {
                'success': True,
                'sql': sql,
                'data': results,
                'formatted': format_results(results),
                'row_count': len(results)
            }
        except Exception as e:
            return {
                'success': False,
                'sql': sql,
                'error': str(e),
                'data': [],
                'formatted': f"执行错误: {str(e)}"
            }

    def execute_with_timeout(self, sql: str, timeout: int = 10) -> Dict[str, Any]:
        """带超时的SQL执行"""
        import threading
        import time

        result = {'success': False, 'error': '超时'}

        def target():
            nonlocal result
            result = self.execute(sql)

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            return {'success': False, 'error': f'执行超时（{timeout}秒）', 'sql': sql}

        return result