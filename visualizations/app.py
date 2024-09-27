import dash
from dash import dcc, html, Input, Output
import pandas as pd
from sqlalchemy import create_engine
from dash import dash_table


# 创建 Dash 应用
app = dash.Dash(__name__)

# 数据库连接信息
DATABASE_URL = "mysql+mysqlconnector://root:0803@localhost:3306/homedepot"
engine = create_engine(DATABASE_URL)

# 查询数据的函数
def fetch_data():
    query = """
        SELECT p.*, 
        DATEDIFF(p.scraped_time, COALESCE(r.SubmissionTime, p.scraped_time)) AS days
        FROM ceiling_fans_with_lights p
        LEFT JOIN reviews r
        ON p.canonicalUrl = r.current_url
        WHERE p.scraped_time = "2024-09-23"
        ORDER BY 
        p.scraped_time ASC;
    """
    df = pd.read_sql(query, engine)
    df = df[['Order', 'label', 'canonicalUrl', 'brandName', 'productLabel', 'price',
         'averageRating', 'totalReviews', 'scraped_time','days']]
    df['total_qty'] = df['totalReviews'].fillna(0) * 100
    df['sales_qty_per_day'] = df['total_qty'] / df['days']
    df['total_sales'] = df['total_qty'] * df['price']
    df['sales_per_day'] = df['total_sales'] / df['days']
    df['days'] = df['days'].fillna(0)  # 将 NaN 替换为 0
    return df

# 应用布局
app.layout = html.Div([
    dcc.Dropdown(
        id='price-range',
        options=[
            {'label': '100-130', 'value': '100-130'},
            {'label': '131-149', 'value': '131-149'},
            {'label': '150-179', 'value': '150-179'},
            {'label': '180-199', 'value': '180-199'},
            {'label': '200-229', 'value': '200-229'},
            {'label': '230-249', 'value': '230-249'},
        ],
        value='100-130',
        clearable=False,
        style={'width': '50%', 'margin': '20px auto'}  # 居中显示
    ),
    dcc.Graph(
        id='sales-pie-chart',
        style={'height': '60vh', 'width': '80vw'}  # 增大饼状图的大小
    ),
    dash.dash_table.DataTable(
        id='table-container',
        columns=[
            {'name': '品牌', 'id': 'productLabel'}, 
            {'name': '销量百分比', 'id': 'pct'}
        ],
        data=[],
        page_size=10,  # 每页显示5行
        style_table={'height': '300px', 'width': '800px', 'overflowY': 'auto', 'margin': 'auto', 'border': 'thin lightgrey solid'},  # 固定高度，启用垂直滚动条
        style_cell={'textAlign': 'left', 'padding': '5px', 'minWidth': '100px', 'width': '150px', 'maxWidth': '200px'},  # 固定列宽
        style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'},
        style_data={'whiteSpace': 'normal', 'height': 'auto'}
    )
])

# 更新图表和表格的回调
@app.callback(
    [Output('sales-pie-chart', 'figure'),
     Output('table-container', 'data')],
    Input('price-range', 'value')
)
def update_output(selected_range):
    df = fetch_data()

    # 根据选择的价格区间过滤数据
    if selected_range == '100-130':
        filtered_df = df[(df['price'] >= 100) & (df['price'] < 130)]
    elif selected_range == '131-149':
        filtered_df = df[(df['price'] >= 131) & (df['price'] < 149)]
    elif selected_range == '150-179':
        filtered_df = df[(df['price'] >= 150) & (df['price'] < 179)]
    elif selected_range == '180-199':
        filtered_df = df[(df['price'] >= 180) & (df['price'] < 199)]
    elif selected_range == '200-229':
        filtered_df = df[(df['price'] >= 200) & (df['price'] < 229)]
    elif selected_range == '230-249':
        filtered_df = df[(df['price'] >= 230) & (df['price'] < 249)]

    # 计算销量百分比
    total_sales = filtered_df['total_sales'].sum()
    filtered_df['pct'] = (filtered_df['total_sales'] / total_sales) * 100 if total_sales > 0 else 0


    # 只显示销量前 N 的产品
    N = 10
    top_products = filtered_df.nlargest(N, 'total_sales')
    other_sales = filtered_df[~filtered_df['label'].isin(top_products['label'])]['total_sales'].sum()

    # 创建“其他”类别的行
    if other_sales > 0:
        other_row = pd.DataFrame({
            'productLabel': ['Others'],
            'total_sales': [other_sales],
            'pct': [other_sales / total_sales * 100]  # 计算其他的百分比
        })
        final_df = pd.concat([top_products, other_row], ignore_index=True)
    else:
        final_df = top_products

    # 创建饼图
    pie_fig = {
        'data': [{
            'labels': final_df['productLabel'],
            'values': final_df['pct'],
            'type': 'pie',
        }],
        'layout': {
            'title': '销量百分比',
        }
    }

    # 返回表格数据
    return pie_fig, final_df[['productLabel', 'pct']].to_dict('records')

# 运行 Dash 应用
if __name__ == '__main__':
    app.run_server(debug=True)