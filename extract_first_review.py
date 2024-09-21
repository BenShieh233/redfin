import requests
import json
from datetime import datetime
from ReviewSQL import Review, Session
import pandas as pd

# 检查 itemId 是否存在于数据库中
def check_item_exists(session, itemId, current_url):
    return session.query(Review).filter_by(itemId=itemId, current_url=current_url).first()

# 保存提取的数据到数据库
def save_to_db(session, review_data):
    # 查找是否已存在该项
    existing_review = check_item_exists(session, review_data['itemId'], review_data['current_url'])
    
    if existing_review:
        # 如果已存在，更新现有记录
        existing_review.ReviewText = review_data['ReviewText']
        existing_review.SubmissionTime = review_data['SubmissionTime']
        print(f"Updated review for item {review_data['itemId']}.")
    else:
        # 如果不存在，则创建新记录
        review = Review(
            itemId=review_data['itemId'],
            current_url=review_data['current_url'],
            ReviewText=review_data['ReviewText'],
            SubmissionTime=review_data['SubmissionTime']
        )
        session.add(review)  # 添加新记录

    session.commit()


def extract_first_review(headers, payload, current_url, itemId):
    session = Session()
    with Session() as session:
        # if check_item_exists(session, itemId, current_url):
        #     print(f"Item {itemId} already exists, skipping...")
        #     return None  # 如果数据已存在，跳过

        headers['X-Current-Url'] = current_url
        payload['variables']['itemId'] = itemId
        url = 'https://apionline.homedepot.com/federation-gateway/graphql?opname=reviews'
        session_req = requests.Session()
        response = session_req.post(url, json=payload, headers=headers)

        if response.status_code == 206:  # 处理 206 Partial Content
            print(f"Received partial content for item {itemId}, saving checkpoint.")
            save_checkpoint(itemId, current_url)  # 保存断点
            return None  # 返回 None 表示未完整获取

        try:
            data = response.json()['data']
            first_review = data.get('reviews', {}).get('Results', [])[0]
            ReviewText = first_review.get('ReviewText', {})
            SubmissionTime = first_review.get('SubmissionTime', {}).split('T')[0]
            SubmissionTime = datetime.strptime(SubmissionTime, '%Y-%m-%d').date()
        except (IndexError, KeyError):
            ReviewText = None
            SubmissionTime = None

        output = {
            'ReviewText': ReviewText,
            'SubmissionTime': SubmissionTime,
            'itemId': itemId,
            'current_url': current_url
        }

        # 保存数据到数据库
        save_to_db(session, output)

        return output
    
# 保存断点
def save_checkpoint(itemId, current_url):
    with open('checkpoint.json', 'w') as file:
        json.dump({'itemId': itemId, 'current_url': current_url}, file)

# 从断点恢复
def load_checkpoint():
    try:
        with open('checkpoint.json', 'r') as file:
            checkpoint = json.load(file)
            return checkpoint['itemId'], checkpoint['current_url']
    except FileNotFoundError:
        return None, None  # 没有断点时返回空值
    
def extract_reviews(df):
    with open('reviews_headers.json', 'r') as f:
        headers = json.load(f)

    with open('reviews_payload.json', 'r') as f:
        payload = json.load(f)

    for index, row in df.iterrows():
        try:
            output = extract_first_review(headers, payload, row['canonicalUrl'], row['itemId'])
            if output is None:
                break
        except Exception as e:
            print(f"error at index {index}, error{e}")
    
# if __name__ == "__main__":
