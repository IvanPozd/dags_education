from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import Variable


def download_file_and_update_schedule(**context):
    """
    Скачивает данные и обновляет расписание
    """
    Variable.set("file_download_status", "downloaded")
    return "process_file_task"


def check_download_status(**context):
    """
    Проверяет статус скачивания и возвращает следующую задачу
    """
    download_status = Variable.get("file_download_status", "not_downloaded")

    if download_status == "not_downloaded":
        # Файл не скачан - используем быстрое расписание
        Variable.set("etl_processing_schedule", "*/5 6,10,14,20 * * *")
        return "download_file_task"
    else:
        # Файл скачан - используем медленное расписание
        Variable.set("etl_processing_schedule", "*/30 6,10,14,20 * * *")
        Variable.set("file_download_status", "not_downloaded")

        return "process_file_task"


etl_processing_schedule = Variable.get(
    "etl_processing_schedule", "*/5 6,10,14,20 * * *"
)

with DAG(
    dag_id="smart_etl_scheduler",
    schedule=etl_processing_schedule,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["GitHub"],
) as dag:
    check_status = BranchPythonOperator(
        task_id="check_download_status",
        python_callable=check_download_status,
    )

    download_file = PythonOperator(
        task_id="download_file_task",
        python_callable=download_file_and_update_schedule,  # из Способа 1
    )

    process_file = BashOperator(
        task_id="process_file_task", bash_command='echo "Обработка данных"'
    )

    check_status >> [download_file, process_file]
    download_file >> process_file
