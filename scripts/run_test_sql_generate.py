#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本 - 任务2：生成问题对应的SQL代码
输出：原始问题、生成的SQL、执行SQL返回的第一个数，以及统计信息
"""

import pandas as pd
import time
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import sys

# 导入项目模块
from sql_generator import SQLGenerator
from executor import SQLExecutor
from retriever import HybridFieldRetriever
import config


class SQLTester:
    """SQL生成测试器 - 用于任务2"""

    def __init__(self):
        """初始化测试器"""
        print("初始化SQL测试器...")
        start_time = time.time()

        # 初始化SQL生成器和执行器
        self.retriever = HybridFieldRetriever()
        self.generator = SQLGenerator(self.retriever)
        self.executor = SQLExecutor()

        init_time = time.time() - start_time
        print(f"初始化完成，耗时: {init_time:.2f}秒")

    def get_first_value(self, result: Dict[str, Any]) -> str:
        """
        从SQL执行结果中提取第一个数值
        """
        if not result.get('success', False):
            return f"ERROR: {result.get('error', 'Unknown error')}"

        data = result.get('data', [])
        if not data:
            return "NO_DATA"

        # 获取第一行数据
        first_row = data[0]
        return str(first_row)

        # # 尝试获取第一个数值
        # try:
        #     # 如果第一行有多个字段，尝试获取第一个数值类型的值
        #     for key, value in first_row.items():
        #         if isinstance(value, (int, float)):
        #             return str(value)
        #     # 如果没有数值，返回第一个字段的值
        #     first_key = list(first_row.keys())[0]
        #     return str(first_row[first_key])
        # except Exception as e:
        #     return f"ERROR: {e}"

    def generate_and_execute(self, question: str) -> Tuple[str, str, float, Dict[str, Any]]:
        """
        生成SQL并执行
        返回：(sql, first_value, 总耗时, 执行详情)
        """

        try:
            # 生成SQL
            sql_start = time.time()
            sql = self.generator.generate_sql(question)
            sql_time = round(time.time() - sql_start, 3)

            # 执行SQL
            exec_result = self.executor.execute(sql)

            # 获取第一个值
            first_value = self.get_first_value(exec_result)

            details = {
                'sql_generation_time': sql_time,
                'success': exec_result.get('success', False),
                'res_count': exec_result.get('res_count', 0),
                'error': exec_result.get('error', ''),
                'first_value': first_value
            }

            return sql, details

        except Exception as e:
            return f"ERROR: {str(e)}"

    def batch_test(self, questions_df: pd.DataFrame, output_path: Path = None,
                   sample_limit: int = None) -> pd.DataFrame:
        """
        测试SQL生成
        """
        results = []
        gen_times = []

        total_questions = len(questions_df)
        if sample_limit:
            total_questions = min(sample_limit, total_questions)
            questions_df = questions_df.head(total_questions)

        print(f"\n开始测试，共 {total_questions} 个问题...")
        print("=" * 80)

        success_count = 0
        error_count = 0

        for idx, row in questions_df.iterrows():
            question = row['question']

            # 生成并执行SQL
            sql, details = self.generate_and_execute(question)

            # 记录结果
            result_row = {
                'index': idx,
                'question': question,
                'generated_sql': sql,
                'first_value': details.get("first_value"),
                'sql_gen_time(s)': details.get('sql_generation_time'),
                'success': details.get('success', False),
                'res_count': details.get('res_count', 0),
                'error': details.get('error', '')
            }

            results.append(result_row)
            gen_times.append(details.get('sql_generation_time'))

            # 统计成功/失败
            if result_row['success']:
                success_count += 1
            else:
                error_count += 1

            # 打印进度
            print(f"已处理 {idx + 1}/{total_questions} 个问题...")
            print("question: ", question)
            print("generated sql: \n", sql)
            print("-" * 50)
            # print(f"当前成功率: {success_count / (idx + 1) * 100:.1f}%")

        # 转换为DataFrame
        results_df = pd.DataFrame(results)

        # 计算统计信息
        if gen_times:
            max_time = max(gen_times)
            min_time = min(gen_times)
            avg_time = sum(gen_times) / len(gen_times)
            total_time_sum = sum(gen_times)

            # 计算SQL生成阶段的平均时间
            avg_gen_time = results_df['sql_gen_time(s)'].mean()
            # avg_exec_time = results_df['sql_exec_time(s)'].mean()

            stats = {
                'index': 'STATS',
                'question': f'统计信息 (成功率: {success_count}/{total_questions})',
                'generated_sql': f'总耗时: {total_time_sum:.3f}s, 最长: {max_time:.3f}s, 最短: {min_time:.3f}s, 平均: {avg_time:.3f}s',
                # 'first_value': f'最长: {max_time:.3f}s, 最短: {min_time:.3f}s, 平均: {avg_time:.3f}s',
                'sql_gen_time(s)': avg_gen_time,
                # 'success': success_count,
                # 'row_count': error_count,
                # 'error': f''
            }
            results_df = pd.concat([results_df, pd.DataFrame([stats])], ignore_index=True)

        # 保存结果
        if output_path:
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n结果已保存到: {output_path}")

        # 打印统计信息
        print("\n" + "=" * 80)
        print("SQL生成测试统计结果:")
        print("=" * 80)
        print(f"问题总数: {total_questions}")
        # print(f"成功生成: {success_count}")
        # print(f"生成失败: {error_count}")
        # print(f"成功率: {success_count / total_questions * 100:.1f}%")
        print(f"\n时间统计:")
        print(f"SQL生成最长总耗时: {max_time:.3f}s")
        print(f"SQL生成最短总耗时: {min_time:.3f}s")
        print(f"SQL生成平均总耗时: {avg_time:.3f}s")
        print(f"SQL生成总耗时: {total_time_sum:.3f}s")

        return results_df


def test_single_question(tester: SQLTester, question: str):
    """测试单个问题（用于调试）"""
    print(f"\n测试问题: {question}")
    print("-" * 50)

    sql, first_value, total_time, details = tester.generate_and_execute(question)

    print(f"生成的SQL:\n{sql}")
    print(f"\n第一个值: {first_value}")
    print(f"总耗时: {total_time * 1000:.2f}ms")
    print(f"SQL生成耗时: {details.get('sql_generation_time', 0) * 1000:.2f}ms")
    print(f"SQL执行耗时: {details.get('sql_execution_time', 0) * 1000:.2f}ms")
    print(f"执行状态: {'成功' if details.get('success') else '失败'}")
    if details.get('error'):
        print(f"错误信息: {details.get('error')}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='测试任务2：生成SQL代码')
    parser.add_argument('--input', type=str, default=r'E:\PAFinQASystem\DATA\示例问题公开.xlsx',
                        help='输入的问题文件路径')
    parser.add_argument('--output', type=str, default=r'E:\PAFinQASystem\PAFinQASystem-ds\results\sql_generation_results.csv',
                        help='输出结果文件路径')
    parser.add_argument('--question-col', type=str, default='question',
                        help='问题列名')
    parser.add_argument('--limit', type=int, default=10,
                        help='限制测试的问题数量（用于快速测试）')
    parser.add_argument('--test_single_question', type=str, default=None,
                        help='测试单个问题（用于调试）')

    args = parser.parse_args()

    # 初始化测试器
    tester = SQLTester()

    # 如果指定了单个问题测试
    if args.test_single_question:
        test_single_question(tester, args.test_single_question)
        return

    # 检查输入文件
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"错误：找不到输入文件 {input_path}")
        return

    print(f"读取问题文件: {input_path}")

    # 读取Excel文件
    try:
        df = pd.read_excel(input_path)
        print(f"读取 {len(df)} 个问题")

        # 检查问题列是否存在
        if args.question_col not in df.columns:
            print(f"错误：找不到列 '{args.question_col}'")
            print(f"可用的列: {list(df.columns)}")
            return

    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # 测试
    output_path = Path(args.output)
    results_df = tester.batch_test(df, output_path, args.limit)

    # # 打印前5个示例结果
    # print("\n" + "=" * 80)
    # print("前5个测试结果示例:")
    # print("=" * 80)
    # for idx, row in results_df.head(5).iterrows():
    #     if row['index'] != 'STATS':
    #         print(f"\n问题 {idx + 1}: {row['question'][:50]}...")
    #         print(f"SQL: {row['generated_sql'][:100]}...")
    #         print(f"第一个值: {row['first_value']}")
    #         print(f"总耗时: {row['total_time_ms']:.2f}ms")

    print("\n测试完成！")


if __name__ == "__main__":
    main()