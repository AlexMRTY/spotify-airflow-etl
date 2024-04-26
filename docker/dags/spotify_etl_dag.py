import sys
sys.path.append('/opt/airflow')

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookHook
from airflow.utils.dates import days_ago
from datetime import timedelta

from scripts.spotify_etl import extract_spotify_data


def notify_on_success(context):
    hook = DiscordWebhookHook(
        http_conn_id="discord_hook",
        message=f"Spotify ETL has been completed successfully! {context['run_id']}",
    )
    hook.execute()

def notify_on_failure(context):
    hook = DiscordWebhookHook(
        http_conn_id="discord_hook",
        message=f"Spotify ETL has failed!{context['exception']}",
    )
    hook.execute()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['asifrezai261@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': notify_on_failure,
}

with DAG(
    dag_id="spotify_etl_dag",
    schedule_interval="@once",
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:
    
    create_table_if_not_exists = PostgresOperator(
        task_id="create_table_if_not_exists",
        sql="sql/create_table.sql",
        postgres_conn_id="postgres_connection",
        on_failure_callback=[notify_on_failure]
    )
    
    extract_spotify_data = PythonOperator(
        task_id="extract_spotify_data",
        python_callable=extract_spotify_data,
        on_success_callback=[notify_on_success]
    )

    start_task = EmptyOperator(task_id="start", dag=dag)
    end_task = EmptyOperator(task_id="end", dag=dag)

    (
        start_task
        >> create_table_if_not_exists
        >> extract_spotify_data
        >> end_task
    )