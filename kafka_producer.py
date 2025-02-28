from kafka import KafkaProducer
import json
from requests import RequestException
import config
import requests


topic_name='tiki_data'
producer=KafkaProducer(
    bootstrap_servers=[config.kafka_config],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')


)



def save_page_to_file(page):
    try:
        with open('C:/Users/Admin/PycharmProjects/Data/ETL_Streaming/save_page.txt','a') as a:
            a.write(str(page)+'\n')
            print(f'Đã lưu page: {page} vào file save_page.txt')
    except Exception as e:
        print(f'Lỗi khi lưu page: {page} vào file save_page.txt',e)




def read_page_from_file():
    try:
        with open('C:/Users/Admin/PycharmProjects/Data/ETL_Streaming/save_page.txt','r') as r:
            lines=r.readlines()
            if lines:
                last_page=int(lines[-1].strip()) if lines else 0
                print(f'Trang cuối cùng đã crawl: {last_page}')
                return last_page
            else:
                return 0
    except Exception as e:
        print('Lỗi khi đọc trang từ file',e)
        return 0
    



def working_link(page):
    link='https://tiki.vn/api/personalish/v1/blocks/listings?'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    params={
        "limit": 40, 
        "include": "advertisement",  
        "aggregations": 2,  
        "version": "home-personalized", 
        "trackity_id": "8f439843-3244-26c8-3181-a6d8714d99ec",  
        "category": 4421,
        "page": page,
        "urlKey": "do-an-vat"
    }
    save_page_to_file(page)
    try:
        response=requests.get(link,params=params,headers=headers,timeout=10)
        response.raise_for_status()
        data=response.json()
        data=data.get('data')
        if data:
            for item in data:
                producer.send(topic_name, item)
            producer.flush()
            print(f'Gửi thành công dữ liệu từ page: {page} tới Kafka')
        else:
            print(f'Không có dữ liệu từ page: {page} tới Kafka')
    except RequestException as e:
        print(f'Lỗi request page: {page}',e)
    except Exception as e:
        print(f'Lỗi không xác định: {e}')






if __name__=='__main__':
    last_crawl_page=read_page_from_file()
    next_page=last_crawl_page+1
    print(f'Đang lấy dữ liệu từ page {next_page}')
    working_link(next_page)