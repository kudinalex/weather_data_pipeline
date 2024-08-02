from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os

# Функция для загрузки данных о погоде
def download_data():
    try:
        api_key = '7e635e7e06819dee7e64869c97739e05'  # Замените на ваш API-ключ
        city = 'Saint Petersburg'  # Город, для которого запрашиваются данные о погоде
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'  # URL для запроса к API
        response = requests.get(url)  # Выполнение HTTP-запроса к API
        data = response.json()  # Преобразование ответа в JSON-формат
        os.makedirs('/tmp', exist_ok=True)  # Создание временной директории для хранения данных
        with open('/tmp/weather_data.json', 'w') as f:  # Открытие файла для записи данных
            json.dump(data, f)  # Сохранение данных в файл
        print("Data downloaded successfully")  # Сообщение об успешной загрузке данных
    except Exception as e:
        print(f"Error in download_data: {e}")  # Вывод сообщения об ошибке, если загрузка данных не удалась

# Функция для обработки загруженных данных
def process_data():
    try:
        with open('/tmp/weather_data.json', 'r') as f:  # Открытие файла для чтения данных
            data = json.load(f)  # Загрузка данных из файла
        df = pd.json_normalize(data)  # Преобразование данных в DataFrame
        df['main.temp'] = df['main.temp'] - 273.15  # Конвертация температуры из Кельвинов в Цельсии
        df.to_csv('/tmp/processed_weather_data.csv', index=False)  # Сохранение обработанных данных в CSV файл
        print("Data processed successfully")  # Сообщение об успешной обработке данных
    except Exception as e:
        print(f"Error in process_data: {e}")  # Вывод сообщения об ошибке, если обработка данных не удалась

# Функция для сохранения обработанных данных
def save_data():
    try:
        df = pd.read_csv('/tmp/processed_weather_data.csv')  # Чтение обработанных данных из CSV файла
        df.to_parquet('/tmp/weather.parquet', index=False)  # Сохранение данных в Parquet файл
        print("Data saved successfully")  # Сообщение об успешном сохранении данных
    except Exception as e:
        print(f"Error in save_data: {e}")  # Вывод сообщения об ошибке, если сохранение данных не удалось

# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',  # Владелец DAG
    'depends_on_past': False,  # Зависимость задач от предыдущих запусков
    'start_date': datetime(2024, 8, 1),  # Дата начала запуска DAG
    'retries': 1,  # Количество попыток повторного выполнения задачи в случае ошибки
    'retry_delay': timedelta(minutes=5),  # Задержка между попытками повторного выполнения задачи
}

# Создание DAG
dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',  # Описание DAG
    schedule_interval='0 0 * * *',  # Расписание запуска DAG (каждый день в полночь)
)

# Определение задачи для загрузки данных
download_task = PythonOperator(
    task_id='download_data',  # Идентификатор задачи
    python_callable=download_data,  # Функция, выполняемая задачей
    dag=dag,  # DAG, к которому принадлежит задача
)

# Определение задачи для обработки данных
process_task = PythonOperator(
    task_id='process_data',  # Идентификатор задачи
    python_callable=process_data,  # Функция, выполняемая задачей
    dag=dag,  # DAG, к которому принадлежит задача
)

# Определение задачи для сохранения данных
save_task = PythonOperator(
    task_id='save_data',  # Идентификатор задачи
    python_callable=save_data,  # Функция, выполняемая задачей
    dag=dag,  # DAG, к которому принадлежит задача
) 

# Определение последовательности выполнения задач
download_task >> process_task >> save_task
