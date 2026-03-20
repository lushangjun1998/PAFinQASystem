import time
import argparse
from sql_generator import SQLGenerator
from executor import SQLExecutor
from retriever import HybridFieldRetriever
from scripts.run_data_prepare import FieldKnowledgeBase


class FinanceQA:
    """金融问答主系统"""

    def __init__(self):
        print("初始化金融问答系统...")
        start_time = time.time()

        # 初始化各组件
        self.retriever = HybridFieldRetriever()
        self.generator = SQLGenerator(self.retriever)
        self.executor = SQLExecutor()

        init_time = time.time() - start_time
        print(f"系统初始化完成，耗时: {init_time:.2f}秒")

    def answer(self, question: str, verbose: bool = False) -> dict:
        """
        回答问题主流程
        """
        total_start = time.time()
        result = {
            'question': question,
            'steps': {},
            'answer': None,
            'error': None
        }

        try:
            # Step 1: 字段检索
            step1_start = time.time()
            fields, tables = self.retriever.retrieve_fields(question)
            step1_time = time.time() - step1_start
            result['steps']['retrieval'] = {
                'time': step1_time,
                'fields_count': len(fields),
                'tables_count': len(tables)
            }

            if verbose:
                print(f"\n步骤1 - 字段检索完成: {step1_time:.3f}秒")
                print(f"召回字段: {[f['field'] for f in fields]}")

            # Step 2: SQL生成
            step2_start = time.time()
            sql = self.generator.generate_sql(question)
            # sql = self.generator.generate_with_retry(question)
            step2_time = time.time() - step2_start
            result['steps']['sql_generation'] = {
                'time': step2_time,
                'sql': sql
            }

            if verbose:
                print(f"\n步骤2 - SQL生成完成: {step2_time:.3f}秒")
                print(f"生成SQL: {sql}")

            # # Step 3: SQL执行
            # step3_start = time.time()
            # exec_result = self.executor.execute_with_timeout(sql)
            # step3_time = time.time() - step3_start
            # result['steps']['execution'] = {
            #     'time': step3_time,
            #     'row_count': exec_result.get('row_count', 0)
            # }
            #
            # if verbose:
            #     print(f"\n步骤3 - SQL执行完成: {step3_time:.3f}秒")
            #     print(f"结果行数: {exec_result.get('row_count', 0)}")
            #
            # # 汇总结果
            # result['answer'] = exec_result.get('formatted', '无结果')
            # if not exec_result['success']:
            #     result['error'] = exec_result.get('error')

        except Exception as e:
            result['error'] = str(e)
            if verbose:
                print(f"\n错误: {e}")

        # 总耗时
        total_time = time.time() - total_start
        result['total_time'] = total_time

        if verbose:
            print(f"\n总耗时: {total_time:.3f}秒")

        return result


def main():
    parser = argparse.ArgumentParser(description='金融问答系统')
    parser.add_argument('--build-index', action='store_true', help='构建向量索引', default=False)
    parser.add_argument('--question', type=str, help='要查询的问题',
                        default="招商基金管理有限公司2019年成立了多少基金?")
    parser.add_argument('--verbose', action='store_true', help='显示详细过程', default=True)

    args = parser.parse_args()

    if args.build_index:
        print("开始构建向量索引...")
        builder = FieldKnowledgeBase()
        builder.build_vector_store()
        print("向量索引构建完成")
        return

    # 初始化QA系统
    qa = FinanceQA()

    if args.question:
        # 单问题模式
        result = qa.answer(args.question, verbose=args.verbose)
        print("\n" + "=" * 50)
        print(f"问题: {result['question']}")
        print(f"答案:\n{result['answer']}")
        if result.get('error'):
            print(f"错误: {result['error']}")
        print(f"总耗时: {result['total_time']:.3f}秒")
    else:
        # 交互模式
        print("\n金融问答系统已启动（输入'exit'退出）")
        while True:
            question = input("\n请输入问题: ").strip()
            if question.lower() in ['exit', 'quit', '退出']:
                break

            result = qa.answer(question, verbose=True)
            print("\n" + "=" * 50)
            print(f"答案:\n{result['answer']}")
            if result.get('error'):
                print(f"错误: {result['error']}")


if __name__ == "__main__":
    main()