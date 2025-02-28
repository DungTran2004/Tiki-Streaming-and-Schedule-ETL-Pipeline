from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta,timezone
import pytz
import subprocess

start_date = datetime.now(pytz.utc) + timedelta(minutes=5)
default_arg={
    'owner':'airflow',
    'retry':1,
    'retry_delay':timedelta(minutes=5),
    'start_date': start_date,
}

dag=DAG(
    dag_id='Tiki_data_schedule',
    default_args=default_arg,
    description='DAG to schedule: crawl tiki product and sended to Kafka',
    schedule_interval='*/5 * * * *',
    catchup=False,

)


def run_kafka_producer():
    subprocess.run(['python','/opt/airflow/dags/ETL_Streaming/kafka_producer.py'])
def run_kafka_consumer():
    subprocess.run(['python','/opt/airflow/dags/ETL_Streaming/load_database.py'])

producer_task=PythonOperator(
    task_id='run_kafka_producer',
    python_callable=run_kafka_producer,
    dag=dag,
)

consumer_task=PythonOperator(
    task_id='run_kafka_consumer',
    python_callable=run_kafka_consumer,
    dag=dag,
)


