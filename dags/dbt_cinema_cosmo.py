from airflow.decorators import dag
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
from pathlib import Path

# Configuração de onde o dbt está no seu Docker/Worker
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/cinema")

profile_config = ProfileConfig(
    profile_name="cinema",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default", # O ID da conexão que você cria no Airflow UI
        profile_args={"schema": "public"},
    ),
)

@dag(
    schedule=None,
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=['dbt', 'cosmos'],
)
def dbt_cinema_cosmos():
    # O Cosmos cria as tasks automaticamente aqui
    projeto_cinema = DbtDag(
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        operator_args={"install_deps": True},
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["path:models/cinema"] # Aqui você filtra o que quer rodar!
        ),
    )

dbt_cinema_cosmos()