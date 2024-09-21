import requests
import json
import pandas as pd
import time
import logging
import random
import os

def extract_data(response):

    try:
        data = response.json().get('data', {})
        
        identifiers = data.get('product', {}).get('identifiers', {})
        fulfillment_options = data.get('product', {}).get('fulfillment', {}).get('fulfillmentOptions', [])
        pricing = data.get('product', {}).get('pricing', {})
        reviews = data.get('product', {}).get('reviews', {})
        
        # Safely extract locations if available
        locations = {}
        if fulfillment_options and 'services' in fulfillment_options[0]:
            locations = fulfillment_options[0]['services'][0].get('locations', [{}])[0]

        # Handle missing fields with defaults
        filtered_locations = {
            'inventory': locations.get('inventory', {}).get('quantity', None),
            'out_of_stock_status': locations.get('inventory', {}).get('isInStock', None)
        }
        filtered_pricing = {
            'value': pricing.get('value', None),
            'original': pricing.get('original', None)
        }

        filtered_identifiers = {
            'storeSkuNumber': identifiers.get('storeSkuNumber', None),
            'brandName': identifiers.get('brandName', None),
            'productLabel': identifiers.get('productLabel', None),
            'canonicalUrl': identifiers.get('canonicalUrl', None),
            'itemId': identifiers.get('itemId', None),
            'parentId': identifiers.get('parentId', None),
            'modelNumber': identifiers.get('modelNumber', None)
        }
        if reviews is not None:
            filtered_reviews = {
                'review_num': reviews.get('ratingsReviews', {}).get('totalReviews', None),
                'rating': reviews.get('ratingsReviews', {}).get('averageRating', None)
            }
        else: 
            filtered_reviews = {
            'review_num': None,
            'rating': None
            }

        # Combine all filtered data into a single result
        results = {**filtered_identifiers, **filtered_pricing, **filtered_locations, **filtered_reviews}

    except Exception as e:
        # Log the error and return empty result if structure is not as expected
        logging.error(f"Error extracting data: {e}")
        results = {
            'storeSkuNumber': None,
            'brandName': None,
            'productLabel': None,
            'canonicalUrl': None,
            'itemId': None,
            'parentId': None,
            'modelNumber': None,
            'inventory': None,
            'out_of_stock_status': None,
            'value': None,
            'original': None,
            'review_num': None,
            'rating': None
        }

    return results


def save_checkpoint(results, file_path='checkpoint.json'):
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=4)


def load_checkpoint(file_path='checkpoint.json'):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    else:
        return []

def hd_scraper(df, headers, payload):
    url = "https://apionline.homedepot.com/federation-gateway/graphql?opname=productClientOnlyProduct"
    results = load_checkpoint()
    processed_ids = {item['itemId'] for item in results}  # 已处理的itemId集合
    session = requests.Session()

    assert '<ID>' in df.columns, "Error: '<ID>' column not found in the Excel sheet."

    for item_id in df['<ID>']:
        if str(item_id) in processed_ids:
            continue  # 跳过已处理的itemId
        
        payload['variables']['itemId'] = item_id
        print(f"现在处于{item_id}")
        time.sleep(15)
        try:
            response = session.post(url, json=payload, headers=headers)

            # 检查cookie失效导致的状态码206
            if response.status_code == 206:
                logging.warning(f"Cookie expired for item {item_id}. Updating cookie...")
                break
                # headers = update_cookie(headers, new_cookie)
                # response = session.post(url, json=payload, headers=headers)

            # 请求成功
            if response.status_code == 200:
                result = extract_data(response)
                if all(value is None for value in result.values()):
                    result['itemId'] = str(item_id)
                results.append(result)
                save_checkpoint(results)  # 每次请求成功后保存checkpoint
                
            else:
                logging.error(f"Failed request for item {item_id}. Status code: {response.status_code}")
        
        except Exception as e:
            logging.error(f"Error occurred for item {item_id}: {e}")

    logging.info(f"Scraping completed. Total results: {len(results)}")
    return results

if __name__ == "__main__":
    with open('headers.json', 'r') as file:
        headers = json.load(file)
    with open('payload.json', 'r') as file:
        payload = json.load(file)

    # Setting up logging for error tracking
    logging.basicConfig(level=logging.INFO)    
    
    df = pd.read_excel(r"C:\Users\cs\OneDrive\Desktop\carro\redfin\Carro-sku.xlsx", sheet_name="每月一次爬取Carro产品信息")
    hd_scraper(df, headers, payload)