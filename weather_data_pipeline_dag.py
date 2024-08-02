from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os

def download_data():
    try:
        api_key = '7e635e7e06819dee7e64869c97739e05'  # Замените на ваш API-ключ
        city = 'Saint Petersburg'
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
        response = requests.get(url)
        data = response.json()
        os.makedirs('/tmp', exist_ok=True)
        with open('/tmp/weather_data.json', 'w') as f:
            json.dump(data, f)
        print("Data downloaded successfully")
    except Exception as e:
        print(f"Error in download_data: {e}")

def process_data():
    try:
        with open('/tmp/weather_data.json', 'r') as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        df['main.temp'] = df['main.temp'] - 273.15  # Convert from Kelvin to Celsius
        df.to_csv('/tmp/processed_weather_data.csv', index=False)
        print("Data processed successfully")
    except Exception as e:
        print(f"Error in process_data: {e}")

def save_data():
    try:
        df = pd.read_csv('/tmp/processed_weather_data.csv')
        df.to_parquet('/tmp/weather.parquet', index=False)
        print("Data saved successfully")
    except Exception as e:
        print(f"Error in save_data: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval='@daily',  # Выполнение каждый день
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
