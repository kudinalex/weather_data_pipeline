from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd

def download_data():
    api_key = 'YOUR_API_KEY'
    city = 'London'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

def process_data():
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    df['main.temp'] = df['main.temp'] - 273.15  # Convert from Kelvin to Celsius
    df.to_csv('/tmp/processed_weather_data.csv', index=False)

def save_data():
    df = pd.read_csv('/tmp/processed_weather_data.csv')
    df.to_parquet('/tmp/weather.parquet', index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval=timedelta(days=1),
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag,
)

download_task >> process_task >> save_task
