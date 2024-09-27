import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging
import os
from datetime import datetime

# 设置日志配置
logging.basicConfig(filename='data_processing.log', level=logging.ERROR,
                    format='%(asctime)s:%(levelname)s:%(message)s')

class ExcelDataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = self.load_data()

    def load_data(self):
        try:
            df = pd.read_excel(self.file_path)
            # 重命名列
            df = df.rename(columns={
                'Label': 'label',
                'Brand': 'brandName',
                'Title': 'productLabel',
                'price': 'price',
                'Review_Number': 'totalReviews',
                'href': 'canonicalUrl',
                'Date': 'scraped_time',
                'Rating': 'averageRating',
                'SKU': 'storeSkuNumber',
                'Model': 'modelNumber',
                'OMSID': 'parentId',
                'Date_of_first_review': 'submissionTime'
            })
            df = df.drop(columns=['Unnamed: 0', 'Review_content', 'Number'])
            
            # 添加SQL中存在但表格中不存在的列，并填充适当的空值
            df['id'] = np.nan
            df['Order'] = np.nan
            df['isSponsored'] = None
            df['inventory'] = None
            df['original_price'] = None
            df['itemId'] = df['parentId']  # 如果 itemId 与 parentId 相同
            
            # 按照给定的SQL顺序重新排列列
            df = df.reindex(columns=['id', 'Order', 'label', 'isSponsored', 'itemId', 
                                     'canonicalUrl', 'brandName', 'productLabel', 
                                     'storeSkuNumber', 'parentId', 'modelNumber', 'price', 
                                     'original_price', 'averageRating', 'totalReviews', 
                                     'inventory', 'scraped_time', 'submissionTime'])

            # 执行类型转换
            type_conversion = {
                'id': 'float64',
                'Order': 'float64',
                'label': 'object',
                'isSponsored': 'float64',
                'itemId': 'object',
                'canonicalUrl': 'object',
                'brandName': 'object',
                'productLabel': 'object',
                'storeSkuNumber': 'object',
                'parentId': 'object',
                'modelNumber': 'object',
                'price': 'float64',
                'original_price': 'float64',
                'averageRating': 'float64',
                'totalReviews': 'float64',
                'inventory': 'float64',
                'scraped_time': 'datetime64[ns]',
                'submissionTime': 'object'
            }

            # 对 df 执行类型转换
            for column, new_type in type_conversion.items():
                df[column] = df[column].astype(new_type)

            # 格式化 submissionTime 列
            df['submissionTime'] = pd.to_datetime(df['submissionTime'], format='%b %d, %Y', errors='coerce')
            df['submissionTime'] = df['submissionTime'].dt.strftime('%Y-%m-%d')

            # 格式化 scraped_time 列
            df['scraped_time'] = pd.to_datetime(df['scraped_time'], errors='coerce')
            df['scraped_time'] = df['scraped_time'].dt.strftime('%Y-%m-%d')

            return df
        
        except Exception as e:
            logging.error(f"Error loading data from Excel: {e}")
            return pd.DataFrame()  # 返回空 DataFrame


class SQLDataProcessor:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)

    def load_sql_data(self, query):
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
                df['scraped_time'] = pd.to_datetime(df['scraped_time'], errors='coerce')
                df['scraped_time'] = df['scraped_time'].dt.strftime('%Y-%m-%d')
                return df
        except Exception as e:
            logging.error(f"Error loading data from SQL: {e}")
            return pd.DataFrame()  # 返回空 DataFrame


class DataMerger:
    def __init__(self, excel_df, sql_df):
        self.excel_df = excel_df
        self.sql_df = sql_df

    def merge_data(self):
        try:
            # 确认列顺序一致，使用 sql_df 的列顺序对 excel_df 重新排列
            self.excel_df = self.excel_df[self.sql_df.columns]
            combined_df = pd.concat([excel_df, self.sql_df], ignore_index=True)
            combined_sorted = combined_df.sort_values(by='scraped_time').reset_index(drop=True)
            return combined_sorted
        except Exception as e:
            logging.error(f"Error merging data: {e}")
            return pd.DataFrame()  # 返回空 DataFrame


class DataUpdater:
    def __init__(self, database_url):
        self.engine = create_engine(database_url)

    def update_data(self, df, table_name):
        try:
            with self.engine.connect() as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                logging.info(f"Updated {table_name} with {len(df)} records.")
        except Exception as e:
            logging.error(f"Error updating SQL table {table_name}: {e}")


def save_combined_data_to_csv(df, filename):
    try:
        df.to_excel(filename, index=False)
        logging.info(f"Combined data saved to {filename}.")
    except Exception as e:
        logging.error(f"Error saving combined data to CSV: {e}")


# 使用示例
if __name__ == "__main__":
    # 定义数据库连接和文件路径
    DATABASE_URL = "mysql+mysqlconnector://root:0803@localhost:3306/homedepot"
    EXCEL_FILE_PATH = 'ceiling_fans_with_lights.xlsx'
    SQL_QUERY = """SELECT c.*, r.submissionTime 
                   FROM homedepot.ceiling_fans_with_lights c
                   LEFT JOIN homedepot.reviews r ON c.canonicalUrl = r.current_url;"""

    # 处理 Excel 数据
    excel_processor = ExcelDataProcessor(EXCEL_FILE_PATH)
    excel_df = excel_processor.df

    # 处理 SQL 数据
    sql_processor = SQLDataProcessor(DATABASE_URL)
    sql_df = sql_processor.load_sql_data(SQL_QUERY)

    # 合并数据
    merger = DataMerger(excel_df, sql_df)
    combined_df = merger.merge_data()
    print(combined_df)
    # 更新数据库
    updater = DataUpdater(DATABASE_URL)
    updater.update_data(combined_df, 'ceiling_fans_with_lights_combined')

    # 生成 CSV 文件名
    excel_filename = f"combined_data_{datetime.now().strftime('%Y%m%d')}.xlsx"
    save_combined_data_to_csv(combined_df, excel_filename)

