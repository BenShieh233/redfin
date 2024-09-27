import json
import requests
import pandas as pd
import time
from datetime import datetime


def update_payload(filename='payload.json', increment=0):
    # 读取源文件
    with open(filename, 'r') as file:
        payload = json.load(file)
    
    # 更新 startIndex
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

def main():

    with open('headers.json', 'r') as file:
        headers = json.load(file)

    with open('payload.json', 'r') as file:
        payload = json.load(file)

    url = 'https://apionline.homedepot.com/federation-gateway/graphql?opname=s'

    initial_increment = 0
    increments = 48  # 每次增量的数量
    num_requests = 5  # 请求次数

    all_results = []

    # 获取当前日期和时间
    current_time = datetime.now().strftime('%Y-%m-%d')

    for i in range(num_requests):

        print(f"now at page {i}")

        session = requests.Session()

        payload = update_payload(increment=initial_increment)

        response = session.post(url, json=payload, headers=headers)

        data = response.json().get('data', {}).get('searchModel', {}).get('products', {})

        results = []
        for product in data:
            product_data = extract_data(product)
            product_data['scraped_time'] = current_time  # 添加时间列
            results.append(product_data)

        all_results.extend(results)

        initial_increment += increments

        time.sleep(15)

    # 将结果保存到 DataFrame
    df = pd.DataFrame(all_results)
    
    # 添加顺序列
    df.reset_index(drop=True, inplace=True)
    df.index += 1  # 顺序从1开始
    df.index.name = 'Order'

    # 保存到 Excel 文件
    df.to_excel('output_results.xlsx', index=True)


if __name__ == '__main__':
    main()