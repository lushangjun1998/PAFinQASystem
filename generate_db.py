import sqlite3
import pandas as pd
import os
from pathlib import Path


def create_database_from_csv(csv_dir='./FinData', db_name='financial_data.db'):
    """
    将FinData目录中的所有CSV文件导入到SQLite数据库中

    Parameters:
    csv_dir: CSV文件所在目录
    db_name: 输出的数据库文件名
    """

    # 定义每个字段对应的SQLite数据类型
    field_types = {
        '所属国家(地区)': 'TEXT',
        '二级行业名称': 'TEXT',
        '第N大重仓股': 'INTEGER',
        '报告期基金总申购份额': 'REAL',
        '市值': 'REAL',
        '到期日期': 'TEXT',
        '持仓日期': 'TEXT',
        '基金代码': 'TEXT',
        '机构投资者持有的基金份额': 'REAL',
        '今开盘(元)': 'REAL',
        '成交量(股)': 'REAL',
        '所在证券市场': 'TEXT',
        '债券名称': 'TEXT',
        '最高价(元)': 'REAL',
        '对应股票代码': 'TEXT',
        '交易日期': 'TEXT',
        '机构投资者持有的基金份额占总份额比例': 'REAL',
        '单位净值': 'REAL',
        '股票名称': 'TEXT',
        '报告期基金总赎回份额': 'REAL',
        '基金全称': 'TEXT',
        '交易日': 'TEXT',
        '成交金额(元)': 'REAL',
        '债券类型': 'TEXT',
        '收盘价(元)': 'REAL',
        '截止日期': 'TEXT',
        '持债数量': 'REAL',
        '持债市值': 'REAL',
        '报告期期初基金总份额': 'REAL',
        '管理费率': 'REAL',
        '管理人': 'TEXT',
        '复权单位净值': 'REAL',
        '持债市值占基金资产净值比': 'REAL',
        '报告期期末基金总份额': 'REAL',
        '市值占基金资产净值比': 'REAL',
        '资产净值': 'REAL',
        '定期报告所属年度': 'TEXT',
        '个人投资者持有的基金份额': 'REAL',
        '个人投资者持有的基金份额占总份额比例': 'REAL',
        '基金类型': 'TEXT',
        '累计单位净值': 'REAL',
        '公告日期': 'TEXT',
        '行业划分标准': 'TEXT',
        '基金简称': 'TEXT',
        '数量': 'REAL',
        '托管费率': 'REAL',
        '托管人': 'TEXT',
        '股票代码': 'TEXT',
        '最低价(元)': 'REAL',
        '昨收盘(元)': 'REAL',
        '成立日期': 'TEXT',
        '一级行业名称': 'TEXT',
        '报告类型': 'TEXT'
    }

    # 连接数据库（如果文件不存在会自动创建）
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # 获取所有CSV文件
    csv_files = sorted(Path(csv_dir).glob('*.csv'))

    print(f"找到 {len(csv_files)} 个CSV文件")

    for csv_file in csv_files:
        try:
            # 获取文件名（不含扩展名）作为表名
            table_name = csv_file.stem
            print(f"\n处理文件: {csv_file.name} -> 表名: {table_name}")

            # 读取CSV文件
            df = pd.read_csv(csv_file, encoding='utf-8')
            print(f"  读取到 {len(df)} 行数据，字段: {list(df.columns)}")

            # 创建表的SQL语句
            create_table_sql = f"CREATE TABLE IF NOT EXISTS [{table_name}] (\n"

            # 为每个字段添加类型定义
            field_definitions = []
            for col in df.columns:
                col_type = field_types.get(col, 'TEXT')  # 默认使用TEXT类型
                field_definitions.append(f"    [{col}] {col_type}")

            create_table_sql += ",\n".join(field_definitions)
            create_table_sql += "\n)"

            # 执行建表语句
            cursor.execute(create_table_sql)
            print(f"  表 {table_name} 创建成功")

            # 将数据逐行插入，确保类型正确
            placeholders = ','.join(['?' for _ in df.columns])
            insert_sql = f"INSERT INTO [{table_name}] ({','.join(['[' + col + ']' for col in df.columns])}) VALUES ({placeholders})"

            # 批量插入数据
            data = [tuple(row) for row in df.values]
            cursor.executemany(insert_sql, data)
            conn.commit()

            # # 将DataFrame数据写入数据库
            # df.to_sql(table_name, conn, if_exists='append', index=False)

            print(f"  成功导入 {len(df)} 条数据到表 {table_name}")

        except Exception as e:
            print(f"  处理文件 {csv_file.name} 时出错: {str(e)}")

    # 获取并显示所有表的信息
    print("\n" + "=" * 50)
    print("数据库创建完成！")

    # 查询所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print(f"\n数据库中的表 ({len(tables)} 个):")
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        row_count = cursor.fetchone()[0]
        print(f"  - {table_name}: {row_count} 行数据")

    # 关闭连接
    conn.close()
    print(f"\n数据库已保存为: {db_name}")


def verify_database(db_name='financial_data.db'):
    """
    验证数据库内容
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("\n" + "=" * 50)
    print("数据库验证:")

    # 获取所有表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"\n表: {table_name}")

        # 获取表结构
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        columns = cursor.fetchall()
        print(f"  字段 ({len(columns)}个):")
        for col in columns:
            print(f"    - {col[1]} ({col[2]})")

        # 获取前3行数据作为示例
        cursor.execute(f"SELECT * FROM [{table_name}] LIMIT 3")
        rows = cursor.fetchall()
        print(f"  数据示例 ({len(rows)}行):")
        for row in rows:
            print(f"    {row}")

    conn.close()


if __name__ == "__main__":
    # 设置文件路径
    csv_directory = r'E:\PAFinQASystem\DATA\FinData'  # CSV文件目录
    database_file = r'E:\PAFinQASystem\DATA\fina_data.db'  # 输出的数据库文件名

    # 检查目录是否存在
    if not os.path.exists(csv_directory):
        print(f"错误: 目录 {csv_directory} 不存在!")
    else:
        # 创建数据库
        create_database_from_csv(csv_directory, database_file)

        # 验证数据库
        verify_database(database_file)