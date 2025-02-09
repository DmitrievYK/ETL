from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

default_args = {
	'owner': 'Yura',
    'depends_on_past': False,	
	'start_date': pendulum.datetime(year=2025, month=2, day=9).in_timezone('Europe/Moscow'),
	'email': ['fireworcks@gmail.com'],	
	'email_on_failure': False,	
	'email_on_retry': False,	
	'retries': 0,	
	'retry_delay': timedelta(minutes=5)
}

#DAG1
dag1 = DAG('sem6_1',
	default_args = default_args,
	description = 'seminar 6',
	catchup = False,
	schedule_interval = '0 6 * * *')

task1 = BashOperator(
    task_id = 'Hello',
    bash_command = 'python3 /home/airflow/dags/sem6/sem6_1.py',
    dag = dag1
)


#DAG2
dag2 = DAG('sem6_2',
	default_args = default_args,
	description = 'seminar 6',
	catchup = False,
	schedule_interval = '0 */6 * * *')

task2 = BashOperator(
    task_id = 'World',
    bash_command = 'python3 /home/airflow/dags/sem6/sem6_2.py',
    dag = dag2
)