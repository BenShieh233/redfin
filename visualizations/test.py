import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class ProductDataProcessor:
    def __init__(self, df):
        self.df = df

    def preprocess_data(self):
        """对数据进行预处理：转换日期格式、处理NaN、计算总价格等"""
        self.df.replace(np.nan, None, inplace=True)

        self.df['scraped_time'] = pd.to_datetime(self.df['scraped_time'], errors='coerce')
        self.df['submissionTime'] = pd.to_datetime(self.df['submissionTime'], errors='coerce')

        # 将 submissionTime 中的 None 值替换为对应的 scraped_time 值
        self.df['submissionTime'].fillna(self.df['scraped_time'], inplace=True)

        # 计算天数差，直接从两个 datetime 列相减
        self.df['days_difference'] = (self.df['scraped_time'] - self.df['submissionTime']).dt.days

        # 根据标签索引前一行的价格填充为0的价格
        self.df['price'] = self.df['price'].replace(0, None)
        self.df['price'] = self.df.groupby('productLabel')['price'].fillna(method='ffill')

        self.df['totalReviews'] = self.df['totalReviews'].fillna(0)
        self.df['price'] = pd.to_numeric(self.df['price'], errors='coerce')
        self.df['totalReviews'] = pd.to_numeric(self.df['totalReviews'], errors='coerce')

        self.df['storeSkuNumber'] = self.df['storeSkuNumber'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
        self.df['parentId'] = self.df['parentId'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)

    def get_top_products(self, top_n=5):
        """获取总评论数乘以价格值最高的前 top_n 个产品"""
        latest_date = self.df['scraped_time'].max()
        latest_data = self.df[self.df['scraped_time'] == latest_date]

        latest_data['price_totalReviews'] = latest_data['price'] * latest_data['totalReviews']

        top_products = latest_data.nlargest(top_n, 'price_totalReviews')
        return top_products
    
    def group_by_week(self, top_products):
        """按周分组，并计算每周 Review_Number 的最小值和平均价格"""
        top_n_data = self.df[self.df['productLabel'].isin(top_products['productLabel'])].drop(columns=['id', 'Order', 'isSponsored'])

        weekly_reviews = top_n_data.groupby(['productLabel', top_n_data['scraped_time'].dt.to_period('W').apply(lambda r: r.start_time)])['totalReviews'].min().reset_index()
        weekly_price = top_n_data.groupby(['productLabel', top_n_data['scraped_time'].dt.to_period('W').apply(lambda r: r.start_time)])['price'].mean().reset_index()

        weekly_data = pd.merge(weekly_reviews, weekly_price, on=['productLabel', 'scraped_time'], how='left')
        # 设置向下移动的周数
        x = 2

        # 将未来 x 周的 totalReviews 向下移动
        weekly_data['Future_Review_Num'] = weekly_data.groupby('productLabel')['totalReviews'].shift(-x)
        # 计算 review_increment: 用当前 Future_Review_Num 减去前一周的值
        weekly_data['review_increment'] = weekly_data.groupby('productLabel')['Future_Review_Num'].diff()            
        
        weekly_data['Sales_increment'] = (weekly_data['review_increment'] * weekly_data['price'] * 100).clip(lower=0)
        
        # 去掉最后两周的 NaN 值
        weekly_data = weekly_data[~weekly_data['Sales_increment'].isna()]

        return weekly_data   
     
    def plot_weekly_data(self, weekly_data):
        """可视化每个产品的每周评论数和平均价格"""
        plt.figure(figsize=(16, 8))

        # 绘制每个产品的每周评论数
        for product, group in weekly_data.groupby('productLabel'):
            plt.plot(group['scraped_time'], group['Sales_increment'], marker='o', linestyle='-', label=f"{product} Reviews")

        # 设置标题和标签
        plt.title('Weekly Sales Amount Increment for Products', fontsize=18)
        plt.xlabel('Week', fontsize=14)
        plt.ylabel('Sales Amount Increment', fontsize=14)

        # 旋转日期标签以便更好地显示
        plt.xticks(rotation=45)

        # 显示图例
        plt.legend(title='Product', loc='upper left')

        # 显示网格
        plt.grid(True)

        # 自动调整布局
        plt.tight_layout()

        # 显示图表
        plt.show()

if __name__ == "__main__":
    df = pd.read_excel('combined_data_20240926.xlsx')
    processor = ProductDataProcessor(df)
    processor.preprocess_data()
    top_products = processor.get_top_products()
    weekly_data = processor.group_by_week(top_products)
    processor.plot_weekly_data(weekly_data)