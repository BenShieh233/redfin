import json
import requests
import pandas as pd
import time
from datetime import datetime
from ReviewSQL import CeilingFanWithLight, Session
import numpy as np

# 函数定义
def save_ceiling_fans_to_db(df):
    # 创建会话
    session = Session()
    
    # 将 DataFrame 中的每一行数据插入到数据库
    for index, row in df.iterrows():
        # 检查记录是否已经存在
        existing_record = session.query(CeilingFanWithLight).filter_by(
            itemId=row['itemId'],
            canonicalUrl=row['canonicalUrl'],
            scraped_time=row['scraped_time']
        ).first()
        
        if existing_record:
            print(f"Record with itemId {row['itemId']}, canonicalUrl {row['canonicalUrl']}, and scraped_time {row['scraped_time']} already exists, skipping...")
            continue  # 如果记录存在，则跳过插入
        ceiling_fan = CeilingFanWithLight(
            itemId=row['itemId'],
            canonicalUrl=row['canonicalUrl'],
            brandName=row['brandName'],
            productLabel=row['productLabel'],
            isSponsored = row['isSponsored'],
            storeSkuNumber=row['storeSkuNumber'],
            parentId=row['parentId'],
            modelNumber=row['modelNumber'],
            price=row['price'],
            original_price=row['original_price'],
            averageRating=row['averageRating'],
            totalReviews=row['totalReviews'],
            inventory=row['inventory'],
            label=row['label'],
            scraped_time=row['scraped_time'],
            Order=row['Order']
        )
        session.add(ceiling_fan)  # 使用 add 而不是 merge
    session.commit()
    print("Data successfully saved to database.")

def update_payload(filename='payload.json', increment=0):
    with open(filename, 'r') as file:
        payload = json.load(file)
    payload['variables']['startIndex'] += increment
    return payload

def extract_data(product):

    itemId = product.get('itemId', {})
    canonicalUrl = product.get('identifiers', {}).get('canonicalUrl', {})
    brandName = product.get('identifiers', {}).get('brandName', {})
    productLabel = product.get('identifiers', {}).get('productLabel', {})
    storeSkuNumber = product.get('identifiers', {}).get('storeSkuNumber', {})
    parentId = product.get('identifiers', {}).get('parentId', {})
    modelNumber = product.get('identifiers', {}).get('modelNumber', {})
    price = product.get('pricing', {}).get('value', {})
    original_price = product.get('pricing', {}).get('original', {})
    isSponsored = product.get('info', {}).get('isSponsored', {})

    

    averageRating = product.get('reviews', {}).get('ratingsReviews', {}).get('averageRating', {})
    totalReviews = product.get('reviews', {}).get('ratingsReviews', {}).get('totalReviews', {})

    try:
        inventory = product.get('fulfillment', {}).get('fulfillmentOptions', {})[1].get('services', {})[0].get('locations', {})[0].get('inventory', {}).get('quantity', {})
    except:
        inventory = None

    try:
        label = product.get('badges', {})[0].get('label', {})
    except:
        label = None
        
    result = {
        'label': label,
        'isSponsored': isSponsored,
        'itemId': itemId,
        'canonicalUrl':canonicalUrl,
        'brandName':brandName,
        'productLabel':productLabel,
        'storeSkuNumber': storeSkuNumber,
        'parentId': parentId,
        'modelNumber': modelNumber,
        'price': price,
        'original_price': original_price,
        'averageRating': averageRating,
        'totalReviews': totalReviews,
        'inventory': inventory

    }

    return result

def scrape_data():
    with open('headers.json', 'r') as file:
        headers = json.load(file)

    with open('payload.json', 'r') as file:
        payload = json.load(file)

    url = 'https://apionline.homedepot.com/federation-gateway/graphql?opname=s'

    initial_increment = 0
    increments = 48  # 每次增量的数量
    num_requests = 5  # 请求次数

    all_results = []
    current_time = datetime.now().strftime('%Y-%m-%d')

    for i in range(num_requests):
        print(f"now at page {i}")

        session = requests.Session()
        payload = update_payload(increment=initial_increment)
        response = session.post(url, json=payload, headers=headers)
        data = response.json().get('data', {}).get('searchModel', {}).get('products', {})

        results = []
        for index, product in enumerate(data):
            product_data = extract_data(product)
            product_data['scraped_time'] = current_time  # 添加时间列
            results.append(product_data)

        all_results.extend(results)
        initial_increment += increments
        time.sleep(15)

    # 将结果保存到 DataFrame
    df = pd.DataFrame(all_results)
    df.reset_index(drop=True, inplace=True)
    df.index += 1  # 顺序从1开始
    df.index.name = 'Order'
    # 将索引列转换为 DataFrame 列
    df.reset_index(inplace=True)
    df = df.replace({np.nan: None})

    save_ceiling_fans_to_db(df)



    df.to_excel(f'ceiling_fans_with_lights_{current_time}.xlsx')

    return df


if __name__ == '__main__':
    # df = pd.read_excel('ceiling_fans_with_lights_2024-09-19.xlsx')
    
    # df = df.replace({np.nan: None})
    # print(df.head(23))
    # print(type(df.iloc[22]['inventory']))
    # df = df.where(pd.notna(df), None)
    # save_ceiling_fans_to_db(df)

    scrape_data()


