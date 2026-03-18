from airflow.decorators import dag, task
from datetime import datetime, timedelta

# Configurações padrão
default_args = {
    'owner': 'Nelio',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_dbt_cinema_decorator',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['dbt', 'cinema', 'analise'],
)
def dbt_cinema_pipeline():

    # @task.bash
    # def dbt_run_staging():
    #     # Roda apenas os modelos na pasta staging (limpeza inicial)
    #     return 'dbt run --project-dir /opt/airflow/dbt/cinema --profiles-dir /opt/airflow/dbt -s staging'

    @task.bash
    def dbt_run_fato_cinema():
        # Roda apenas um modelo específico pelo nome do arquivo .sql
        return 'dbt run --project-dir /opt/airflow/dbt/cinema --profiles-dir /opt/airflow/dbt -s salas'

    @task.bash
    def dbt_test():
        # Roda os testes de qualidade (not null, unique) apenas no que mudou
        return 'dbt test --project-dir /opt/airflow/dbt/cinema --profiles-dir /opt/airflow/dbt'

    # Definindo a ordem de execução
    # run_staging = dbt_run_staging()
    run_fato_cinema = dbt_run_fato_cinema()
    run_test = dbt_test()

    run_fato_cinema >> run_test

# Chamada da função para registrar a DAG
dbt_cinema_pipeline()