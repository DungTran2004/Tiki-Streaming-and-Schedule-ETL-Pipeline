
import mysql.connector
import pandas as pd
from kafka import KafkaConsumer
import config
import json


consumer=KafkaConsumer(
    'tiki_data',
    bootstrap_servers=[config.kafka_config],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my_consumer_group',
    consumer_timeout_ms=10000
)


def product(massage):
    product_data=[massage.get('id'),massage.get('sku'),massage.get('name'),massage.get('brand_name'),massage.get('short_description')]
    return product_data
def price(massage):
    price_data=[massage.get('id'),massage.get('price'),massage.get('discount'),massage.get('discount_rate'),massage.get('original_price')]
    return price_data
def sell_volumn(massage):
    sell_volume_dict=massage.get('quantity_sold',{})
    sell_volume_dict['product_id']=massage.get('id')
    return sell_volume_dict

batch_size=20
batch_data={'product':[],'price':[],'product_volume':[]}

product_df=pd.DataFrame(columns=['id','sku','name','brand_name','short_description'])
price_df=pd.DataFrame(columns=['product_id','price','discount','discount_rate','original_price'])
sell_volume_df=pd.DataFrame(columns=['text','product_id','value'])
price_df['discount']=price_df['discount'].astype(float)
price_df['discount_rate']=price_df['discount_rate'].astype(float)
price_df['price']=price_df['price'].astype(float)
price_df['original_price']=price_df['original_price'].astype(float)

for massage in consumer:
    message = json.loads(massage.value.decode('utf-8'))
    batch_data['product'].append(product(message))
    batch_data['price'].append(price(message))
    batch_data['product_volume'].append(sell_volumn(message))
    if len(batch_data['product'])>=batch_size:
        product_df=pd.concat([product_df,pd.DataFrame(batch_data['product'],columns=product_df.columns)],ignore_index=True)
        price_df=pd.concat([price_df,pd.DataFrame(batch_data['price'],columns=price_df.columns)],ignore_index=True)
        sell_volume_df=pd.concat([sell_volume_df,pd.DataFrame(batch_data['product_volume'],columns=sell_volume_df.columns)],ignore_index=True)
        print('----20 Request Done-----')
        batch_data={'product':[],'price':[],'product_volume':[]}
    else:
        continue



def mysql_connection():
    try:
        connect=mysql.connector.connect(
            host=config.mysql_config['host'],
            user=config.mysql_config['user'],
            password=config.mysql_config['password'],
            database=config.mysql_config['database']
        )
        return connect
    except mysql.connector.Error as err:
        print("Lỗi kết nối tới MySQL: %s", err)
        return None

def load_to_db(data,table,connect):
    if data.empty:
        print('Khong co du lieu')
        return
    cursor=connect.cursor()
    cols = ",".join(list(data.columns))
    placeholders = ",".join(["%s"] * len(data.columns))
    insert_stmt = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    try:
        for i, row in data.iterrows():
            cursor.execute(insert_stmt, tuple(row))
        connect.commit()
        print(f"Chèn thành công dữ liệu vào bảng {table}")
    except mysql.connector.Error as e:
        print("Lỗi khi chèn dữ liệu vào %s: %s", table, e)
        connect.rollback()
    finally:
        cursor.close()


if __name__=='__main__':
    connect=mysql_connection()
    if connect:
        #load_to_db(product_df,'product',connect)
        load_to_db(price_df,'price',connect)
        #load_to_db(sell_volume_df,'sell_volume',connect)
        connect.close()
    else:
        print('Khong the ket noi toi MySQL')

