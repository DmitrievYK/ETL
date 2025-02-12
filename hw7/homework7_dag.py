import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
import re


def hw7_get_temp(**kwargs):
    latitude = 55.7522
    longitude = 37.6156
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('current_weather').get('temperature')
    else:
        raise Exception("Error fetching weather data")

def hw7_check_temp(ti):
    temp = ti.xcom_pull(task_ids='hw7_get_temp')
    print(f'Temperature now is {temp}')
    temp = round(float(temp))
    if temp >= 15:
        return 'hw7_warm'
    else:
        return 'hw7_cold'


with DAG(
        'hw_7_dag',
        start_date=datetime(2024, 11, 1),
        schedule_interval='0 12 * * *',
        catchup=False
) as dag:

    hw7_get_temp = PythonOperator(
        task_id='hw7_get_temp',
        python_callable=hw7_get_temp,
    )

    hw7_check_temp = BranchPythonOperator(
        task_id='hw7_check_temp',
        python_callable=hw7_check_temp,
    )

    hw7_warm = BashOperator(
        task_id='hw7_warm',
        bash_command='echo "Тепло"',
    )

    hw7_cold = BashOperator(
        task_id='hw7_cold',
        bash_command='echo "Холодно"',
    )

    hw7_get_temp >> hw7_check_temp >> [hw7_warm, hw7_cold]