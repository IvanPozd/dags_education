from airflow.operators.python import BranchPythonOperator

def check_download_status(**context):
    """
    Проверяет статус скачивания и возвращает следующую задачу
    """
    download_status = Variable.get("file_download_status", "not_downloaded")
    
    if download_status == "not_downloaded":
        # Файл не скачан - используем быстрое расписание
        Variable.set("etl_processing_schedule", "*/5 6,10,14,20 * * *")
        return 'download_file_task'
    else:
        # Файл скачан - используем медленное расписание
        Variable.set("etl_processing_schedule", "*/30 6,10,14,20 * * *")
        return 'process_file_task'

with DAG(
    dag_id='smart_etl_scheduler',
    schedule_interval="*/5 6,10,14,20 * * *",  # Начальное быстрое расписание
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    check_status = BranchPythonOperator(
        task_id='check_download_status',
        python_callable=check_download_status,
        provide_context=True
    )

    download_file = PythonOperator(
        task_id='download_file_task',
        python_callable=download_file_and_update_schedule  # из Способа 1
    )

    process_file = BashOperator(
        task_id='process_file_task',
        bash_command='echo "Обработка данных"'
    )

    check_status >> [download_file, process_file]
    download_file >> process_file
