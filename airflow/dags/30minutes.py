from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False, 
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    dag_id= 'usersign_kafka_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

run_this_second = BashOperator(
    task_id='run_this_second',
    bash_command='java --version ',
    dag=dag,
)

now_run_spark_job = BashOperator(
    task_id='now_run_spark_job',
    bash_command='bash /Users/aminu/airflow/dags/spark.sh ',
    dag=dag,
)
run_this_first >> run_this_second >> now_run_spark_job
