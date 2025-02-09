# Домашняя работа 6

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import random
import json


dag = DAG(
	'hw_6',
	description = 'Homework 6',
	schedule_interval = '*/5 * * * *',
	start_date = datetime(2025, 2, 9),
	catchup = False
)


# 1. Создайте новый граф. Добавьте в него BashOperator, который будет генерировать 
# рандомное число и печатать его в консоль.

hw_6_1_random = BashOperator(
	task_id="hw_6_1_random",
	bash_command='echo $RANDOM',
	dag=dag,
)


# 2. Создайте PythonOperator, который генерирует рандомное число, возводит его 
# в квадрат и выводит в консоль исходное число и результат.

def get_random():
	number = random.randrange(10)
	print(f"Number: {number}")
	print(f"Squared number: {number**2}")

hw_6_2_random_square = PythonOperator(
	task_id="hw_6_2_random_square",
	python_callable=get_random,
	dag=dag,
)


# 3. Сделайте оператор, который отправляет запрос к 
# https://goweather.herokuapp.com/weather/"location" 
# (вместо location используйте ваше местоположение).

hw_6_3_get_weather = SimpleHttpOperator(
        task_id='hw_6_3_get_weather',
        http_conn_id='weather_api',
        endpoint='/weather/Moscow',
        method='GET',
        data=json.dumps({"priority": 5}),
		dag = dag
    )

def weather_print(**kwargs):
    data = kwargs['task_instance'].xcom_pull(task_ids='hw_6_3_get_weather')
    data_dict = json.loads(data)
    print(f"Температура в Москве {data_dict.get('temperature')}")
    return data
 


hw_6_3_weather_print = PythonOperator(
        task_id=f'hw_6_3_print_weather',
        python_callable=weather_print,
        provide_context=True,
        dag = dag
    )

hw_6_1_random >> hw_6_2_random_square >> hw_6_3_get_weather >> hw_6_3_weather_print