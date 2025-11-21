# main_etl_dag.py
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import (
    PythonOperator,
)
from airflow.providers.standard.sensors.filesystem import FileSensor


def get_current_schedule():
    """Получает текущее расписание из Variable"""
    return Variable.get("etl_processing_schedule", default_var="*/5 6,10,14,20 * * *")


def mark_file_downloaded(**context):
    """Помечает файл как скачанный и обновляет расписание"""

    # Создаем маркерный файл
    with open("/tmp/file_downloaded.marker", "w") as f:
        f.write("downloaded")

    Variable.set("file_download_status", "downloaded")
    Variable.set("etl_processing_schedule", "*/30 6,10,14,20 * * *")

    return "File downloaded and schedule updated"


# Динамическое расписание
current_schedule = get_current_schedule()

with DAG(
    dag_id="main_etl_processor",
    schedule=current_schedule,  # Динамическое расписание
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "production"],
) as dag:
    # Сенсор проверяет, был ли файл уже скачан
    check_download_sensor = FileSensor(
        task_id="check_file_downloaded",
        filepath="/tmp/file_downloaded.marker",
        mode="reschedule",
        timeout=300,
        poke_interval=60,
    )

    download_file = PythonOperator(
        task_id="download_data_file",
        python_callable=mark_file_downloaded,
    )

    process_data = BashOperator(
        task_id="process_data",
        bash_command='echo "Обработка данных..." && sleep 30',
    )

    # Если файл уже скачан - пропускаем скачивание
    check_download_sensor >> process_data
    download_file >> process_data
