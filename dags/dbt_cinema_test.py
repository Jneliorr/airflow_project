from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configurações básicas
default_args = {
    'owner': 'Nelio',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_dbt_cinema_test',
    default_args=default_args,
    description='Executa o projeto dbt cinema',
    schedule_interval=None, # Rodar manualmente por enquanto
    catchup=False,
) as dag:

    # 1. Testar a conexão (O debug que fizemos antes)
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='dbt debug --project-dir /opt/airflow/dbt/cinema --profiles-dir /opt/airflow/dbt'
    )

    # 2. Rodar os modelos (Cria as tabelas/views no banco)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/dbt/cinema --profiles-dir /opt/airflow/dbt'
    )

    # Fluxo: Primeiro o debug, depois o run
    dbt_debug >> dbt_run