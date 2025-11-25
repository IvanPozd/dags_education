import json

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def get_json_data_from_source(source_file_path: str, mode: str = "r"):
    with open(source_file_path, mode=mode) as json_file:
        return json.load(json_file)


with DAG(
    dag_id="download_rocket_launches",
    schedule="@hourly",
    tags=["GitHub"],
) as dag:
    download = BashOperator(
        task_id="download_launches_task",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    read = PythonOperator(
        task_id="read_launches_file_task",
        python_callable=lambda: print(get_json_data_from_source("/tmp/launches.json")),
    )

    download >> read
