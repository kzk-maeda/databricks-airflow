import boto3
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator


def on_failure_callback(context):
    pass

def on_success_callback(context):
    pass

########################
## Airflow Default Args
args = {
    "owner": "airflow",
    "provide_context": True,
    "depends_on_past": False,  # 指定したタスクの上流タスクの実行が失敗した場合タスクを実行するか
    "on_failure_callback": on_failure_callback,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120),
}

with DAG(
    dag_id="sample",
    default_args=args,
    schedule_interval="00 16 * * *",
    start_date=datetime(2022, 6, 14, 00, 00, 00),
    catchup=False,  ## backfillしない
) as dag:
    
    task_bash_end = BashOperator(
        task_id="task_end",
        bash_command="echo end",
        on_success_callback=on_success_callback,
    )

    task_bash_end