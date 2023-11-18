from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
        'owner' : 'Hamed',
        'start_date' : datetime(2022, 11, 12),
}


dag = DAG(dag_id='wisdomise_database',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )
################################################################
start = DummyOperator(task_id = 'start', dag = dag)
sensor = DummyOperator(task_id = 'pysensor1_trigger', dag = dag)
################################################################

###############################################################
end = DummyOperator(task_id = 'end', dag = dag)


start >> sensor >> end