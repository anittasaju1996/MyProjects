from airflow import DAG 
from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta


default_args = {
    'owner' : 'anitta',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1),
    'depends_on_past' : True
}

#getting mysql connection params
mysql_conn_obj = BaseHook.get_connection('mysql_conn')

mysql_host=mysql_conn_obj.host
mysql_port=mysql_conn_obj.port
mysql_user=mysql_conn_obj.login
mysql_password=mysql_conn_obj.password


with DAG(
    dag_id='insert_data_into_mysql',
    description='insert data into mysql using pyspark ',
    start_date=datetime(year=2022, month=12, day=21, hour=1, minute=30),
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    task1 = BashOperator(task_id='greeting', bash_command='whoami')

    task2 = SSHOperator(
        task_id='spark_submit',
        ssh_conn_id='ssh_spark',
        command=f'''. /home/spark_user/docker_env.txt && spark-submit \
            --master spark://spark:7077 \
            --deploy-mode client \
            --executor-memory 4g \
            --executor-cores 2 \
            --driver-cores 1\
            --driver-memory 1g \
            --jars "/opt/bitnami/spark/dev/jars/mysql-connector-j-8.0.31.jar" \
            /opt/bitnami/spark/dev/scripts/pyspark_trial.py {mysql_host} {mysql_port} {mysql_user} {mysql_password}
            '''
    )

    task3 = BashOperator(task_id='fin', bash_command='echo completed')


    task1 >> task2 >> task3