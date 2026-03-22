#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试脚本 - 任务1：匹配问题中的指标维度名
输出：匹配时间、匹配的维度名、表名，以及统计信息
"""

import pandas as pd
import time
from pathlib import Path
from typing import List, Dict, Any, Tuple

# 导入项目模块
from RAG.retriever import HybridFieldRetriever
from utils import load_json
import config


class FieldMatcher:
    """字段匹配器 - 用于任务1"""

    def __init__(self):
        """初始化检索器"""
        print("初始化字段匹配器...")
        start_time = time.time()

        # 初始化混合检索器
        self.retriever = HybridFieldRetriever()

        # 加载字段知识库（用于获取字段详细信息）
        self.field_knowledge = load_json(config.FIELD_KNOWLEDGE_PATH)

        init_time = time.time() - start_time
        print(f"初始化完成，耗时: {init_time:.2f}秒")

    def match_fields(self, question: str) -> Tuple[List[Dict[str, Any]], float]:
        """
        匹配问题中的字段
        返回：(匹配的字段列表, 耗时)
        """
        start_time = time.time()

        # 使用检索器获取相关字段
        pages, fields, tables = self.retriever.retrieve_fields(question)

        elapsed_time = round(time.time() - start_time, 3)

        return fields, elapsed_time

    def batch_match(self, questions_df: pd.DataFrame, output_path: str = None) -> pd.DataFrame:
        """
        批量匹配问题
        """
        results = []
        match_times = []

        print(f"\n开始匹配，共 {len(questions_df)} 个问题...")
        print("=" * 80)

        for idx, row in questions_df.iterrows():
            question = row['question']

            # 匹配字段
            fields, match_time = self.match_fields(question)

            matched_fields = [field['table'] + '-' + field['field'] for field in fields]
            # 记录结果
            result_row = {
                'index': idx,
                'question': question,
                'match_time(s)': match_time,
                'matched_fields': ', '.join(matched_fields)
            }

            results.append(result_row)
            match_times.append(match_time)

            print(f"已处理 {idx + 1}/{len(questions_df)} 个问题...")
            print("question: ", question)
            print("matched fields: ", ', '.join(matched_fields))
            print("-" * 50)

        # 转换为DataFrame
        results_df = pd.DataFrame(results)

        # 计算统计信息
        if match_times:
            max_time = max(match_times)
            min_time = min(match_times)
            avg_time = round(sum(match_times) / len(match_times), 3)
            total_time = round(sum(match_times), 3)

            # 添加统计行
            stats = {
                'index': 'STATS',
                'question': '统计信息',
                'match_time(s)': total_time,
                'matched_fields': f'最长: {max_time:.3f}s, 最短: {min_time:.3f}s, 平均: {avg_time:.3f}s',
            }
            results_df = pd.concat([results_df, pd.DataFrame([stats])], ignore_index=True)

        # 保存结果
        if output_path:
            results_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n结果已保存到: {output_path}")

        # 打印统计信息
        print("\n" + "=" * 80)
        print("匹配统计结果:")
        print("=" * 80)
        print(f"问题总数: {len(questions_df)}")
        print(f"最长匹配时间: {max_time:.3f}s")
        print(f"最短匹配时间: {min_time:.3f}s")
        print(f"平均匹配时间: {avg_time:.3f}s")
        print(f"总匹配时间: {total_time:.3f}s")
        return results_df

def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description='任务1：匹配指标维度名')
    parser.add_argument('--input', type=str, default=r'E:\PAFinQASystem\DATA\示例问题公开.xlsx',
                        help='输入的问题文件路径')
    parser.add_argument('--output', type=str, default=r'E:\PAFinQASystem\PAFinQASystem-ds\results\task1_retrieval_results.csv',
                        help='输出结果文件路径')
    parser.add_argument('--question-col', type=str, default='question',
                        help='问题列名')
    parser.add_argument('--verbose', action='store_true', help='显示详细过程', default=True)

    args = parser.parse_args()

    # 检查输入文件
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"错误：找不到输入文件 {input_path}")
        return

    # 读取Excel文件
    try:
        df = pd.read_excel(input_path)
        print(f"成功读取 {len(df)} 个问题")


        # 检查问题列是否存在
        if args.question_col not in df.columns:
            print(f"错误：找不到列 '{args.question_col}'")
            print(f"可用的列: {list(df.columns)}")
            return

    except Exception as e:
        print(f"读取文件失败: {e}")
        return

    # 初始化匹配器
    matcher = FieldMatcher()

    # 检索
    output_path = Path(args.output)
    results_df = matcher.batch_match(df, output_path)

    print("\n测试完成！")


if __name__ == "__main__":
    main()