from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

# Caminhos locais (Ajuste para o seu projeto de cinema)
CAMINHO_ORIGEM = '/opt/airflow/cinema2026/data/raw/zip/bilheteria-diaria-obras-por-exibidoras-csv.zip'
CAMINHO_DESTINO = '/opt/airflow/cinema2026/data/raw/arquivo/zip/bilheteria-diaria-obras-por-exibidoras'

@dag(
    dag_id='teste_movimentacao_cinema',
    schedule=None,  # Execução manual para o seu teste
    start_date=datetime(2026, 3, 7),
    catchup=False,
    tags=['cinema', 'local', 'test'],
)
def pipeline_teste():

    @task
    def inicio():
        print("Iniciando o teste de movimentação de arquivos ZIP...")

    # Integrando o BashOperator dentro da estrutura de Decorators
    mover_zips = BashOperator(
        task_id='mover_zips_cinema',
        # bash_command=f'mv {CAMINHO_ORIGEM} {CAMINHO_DESTINO}'
        bash_command=f'mv /opt/airflow/cinema2026/data/raw/zip/bilheteria-diaria-obras-por-exibidoras-csv.zip opt/airflow/cinema2026/data/raw/arquivo/zip/bilheteria-diaria-obras-por-exibidoras/ '

    )

    @task
    def fim():
        print("Arquivos movidos com sucesso!")

    # Definindo a ordem de execução
    inicio() >> mover_zips >> fim()

# Instanciando a DAG
pipeline_teste()