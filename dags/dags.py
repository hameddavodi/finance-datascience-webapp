from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from kafka import *
from Core import *
import json

default_args = {
        'owner' : 'Hamed',
        'start_date' : datetime(2022, 11, 12),
}

dag = DAG(dag_id='wisdomise_database_semi_final',
        default_args=default_args,
        start_date=datetime(2023, 4, 1),
        schedule="@continuous",
        max_active_runs=1,
        catchup=False,
        render_template_as_native_obj=True,
    )
################################################################
start = DummyOperator(task_id = 'start', dag = dag)
################################################################
# Here I should specify to continuously listen to the kafka for incoming messages
# I could have puy python Sensor but the logic behind reciving kafka is itself a sensor

def check_connection(**context):
    
    bootstrap_servers = "broker:29092"
    group_id = "unique_1"
    topic = "wisdomise_broker"
    msg,_ = kafka_consumer(bootstrap_servers, group_id, topic)
    context['ti'].xcom_push(key='my_key', value = msg)


recieve_msg = PythonOperator(task_id = 'get_start_date',
                       python_callable=check_connection,
                       provide_context=True,
                       dag = dag)

################################################################
def send_recieved():
    
    broker = kafka_broker("We successfully recieved start date")
    broker.send_start_date()

send_msg = PythonOperator(task_id = 'send_success',
                       python_callable=send_recieved,
                       dag = dag)

################################################################
def write_psycop(**context):
   value = context['ti'].xcom_pull(key='my_key')
   start = value
   print(start)
   start, end = intervals(start)
   engine = connect_to_database()
   write_sql_table(start,end)

writng = PythonOperator(task_id = 'pyoperator_writing_api',
                       python_callable= write_psycop,
                       provide_context=True,
                       dag = dag)

###############################################################
end = DummyOperator(task_id = 'end', dag = dag)

start >> recieve_msg >> send_msg >> writng >> end