
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json


WEATHER_API_KEY = Variable.get("weather_api_key")
BUCKET_NAME = Variable.get("bucket_name_weather")
lat = 52.5214
lon = 13.4339
default_args = {
    'owner': 'oliver',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pull_weather_data',
    default_args=default_args,
    description='Pulls weather data from API and stores in S3 bucket hourly',
    schedule_interval=timedelta(minutes=2)
)

def get_weather_data():
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_API_KEY}'
    response = requests.get(url)
    data = json.loads(response.text)
    return data

def upload_to_s3():
    hook = S3Hook('s3_connection')
    data = get_weather_data()
    file_name = f'weather_data_{datetime.now().strftime("%Y-%m-%d_%H:%M:%S")}.json'
    hook.load_string(json.dumps(data), key=file_name, bucket_name=BUCKET_NAME)

with dag:
    
    t1 = PythonOperator(
        task_id='get_weather_data',
        python_callable=get_weather_data
    )

    t2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    t1 >> t2