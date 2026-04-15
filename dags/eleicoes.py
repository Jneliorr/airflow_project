from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param
from airflow.sdk.definitions.param import ParamsDict
from sqlalchemy import create_engine, VARCHAR, Integer,Date, inspect
import os
import pandas as pd
from datetime import datetime
import zipfile
import numpy as np
import requests
from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime
from pathlib import Path
import shutil
import boto3
from botocore.config import Config



# Capturando as variáveis do seu ambiente Docker
USER = os.getenv('DB_USER')
PASSWORD = os.getenv('DB_PASSWORD')

params = {
        "Usuario": Param(
        default="José Nélio"
        ,type='string'
        ,enum=["José Nélio"]
        ,description="""
        Escolher usuario 
        """
        )

    ,'database': Param(
        default='eleicao_RR'
        ,type="string"
        ,description="Escolher o nome do banco de archives para subir os archives tratados")

    }

# Configuração de onde o dbt 

DBT_PROJECT_PATH = Path("/opt/airflow/dbt/eleicao")

profile_config = ProfileConfig(
    profile_name="eleicao",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default", # O ID da conexão 
        profile_args={"schema": "public"},
    ),
)


default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dag_eleicao",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    params=params,
    catchup=False,
    tags=["cinema"],
    max_active_tasks= 30,
    default_args=default_args
)


def eleicao():
    
    @task
    def unzip_file(zip_path, file_save):
        os.makedirs(file_save, exist_ok=True)

        for file_name in os.listdir(zip_path):
            if file_name.lower().endswith('.zip'):
                zip_file = os.path.join(zip_path, file_name)
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        nome_arquivo = os.path.basename(member)
                        destino_final = os.path.join(file_save, nome_arquivo)
                        with zip_ref.open(member) as origem, open(destino_final, 'wb') as destino:
                            shutil.copyfileobj(origem, destino)
                        print(f"Extraído: {nome_arquivo}")



    @task
    def local_votacao (path):
        engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@db:5432/eleicao_RR")
        colunas_leitura = [
        'dt_geracao', 'hh_geracao', 'aa_eleicao', 'dt_eleicao', 'ds_eleicao',
        'nr_turno', 'sg_uf', 'cd_municipio', 'nm_municipio', 'nr_zona',
        'nr_secao', 'cd_tipo_secao_agregada', 'ds_tipo_secao_agregada',
        'nr_secao_principal', 'nr_local_votacao', 'nm_local_votacao',
        'cd_tipo_local', 'ds_tipo_local', 'ds_endereco', 'nm_bairro', 'nr_cep',
        'nr_telefone_local', 'nr_latitude', 'nr_longitude',
        'cd_situ_local_votacao', 'ds_situ_local_votacao', 'cd_situ_zona',
        'ds_situ_zona', 'cd_situ_secao', 'ds_situ_secao', 'cd_situ_localidade',
        'ds_situ_localidade', 'cd_situ_secao_acessibilidade',
        'ds_situ_secao_acessibilidade', 'qt_eleitor_secao',
        'qt_eleitor_eleicao_federal', 'qt_eleitor_eleicao_estadual',
        'qt_eleitor_eleicao_municipal', 'nr_local_votacao_original',
        'nm_local_votacao_original', 'ds_endereco_locvt_original',
        'id_zona_secao_municipio'
        ]
        dfs = []
        colunas_num = ['qt_eleitor_secao', 'qt_eleitor_eleicao_federal', 'qt_eleitor_eleicao_estadual','qt_eleitor_eleicao_municipal']
        colunas_data = ['dt_geracao','dt_eleicao']
        colunas_hora = ['hh_geracao']
        for bweb in os.listdir(path):
            full_path = os.path.join(path, bweb)
            if full_path.endswith('.csv'):
                print(f"Lendo o arquivo: {full_path}")
                df = pd.read_csv(full_path, encoding='latin-1', sep=';',dtype=str,usecols=lambda col: col.strip().lower() in colunas_leitura, nrows=10)
                df.columns = [col.strip().lower() for col in df.columns]
                for col in colunas_num:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col]).astype('Int64')
                for col in colunas_data:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], dayfirst=True, format='%d/%m/%Y', errors='coerce').dt.date
                for col in colunas_hora:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
                df['aa_eleicao'] = df['aa_eleicao'].astype(str).str.lstrip("0")
                df['id_zona_secao_municipio'] = df['aa_eleicao'] +  df['nr_turno'] + df['nr_zona'] +  df['nr_secao'] +  df['cd_municipio']
                df.to_sql("perfil_eleitorado", engine, index=False, if_exists='append')

                dfs.append(df)




            return print("Todos os arquivos foram processados e carregados no banco de archives com sucesso!")

    @task
    def perfil (file_csv):
        engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@db:5432/eleicao_RR")
        dfs = []
        colunas_num = ['QT_ELEITORES_INC_NM_SOCIAL', 'QT_ELEITORES_DEFICIENCIA', 'QT_ELEITORES_BIOMETRIA','QT_ELEITORES_PERFIL']
        colunas_data = ['DT_GERACAO']
        colunas_hora = ['HH_GERACAO']
        for bweb in os.listdir(file_csv):
            full_path = os.path.join(file_csv, bweb)
            if full_path.endswith('.csv'):
                print(f"Lendo arquivo: {full_path}")
                df = pd.read_csv(full_path, encoding='latin-1', sep=';',dtype=str,nrows=10)
                for col in colunas_num:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col]).astype('Int64')
                for col in colunas_data:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col],  dayfirst=True, format='%d/%m/%Y').dt.date
                for col in colunas_hora:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.time
                df['ID_ZONA_SECAO_MUNICIPIO'] = df['ANO_ELEICAO'] + df['NR_ZONA'] +  df['NR_SECAO'] +  df['CD_MUNICIPIO']
                dfs.append(df)
                print(f"Carregando o arquivo no banco de archives: {bweb}")
                df.columns = [col.lower() for col in df.columns]
                df.to_sql("perfil_eleitorado", engine, index=False, if_exists='append')
                print(f"Arquivo Carregado no Bando de archives: {bweb}")

        return print("Todos os arquivos foram processados e carregados no banco de archives com sucesso!")
    

    @task
    def turnos (file_csv):
        engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@db:5432/eleicao_RR")
        dfs = []
        try:
            colunas_resultado = {col['name'] for col in inspect(engine).get_columns('resultado')}
        except Exception as err:
            print(f"Nao foi possivel ler o schema da tabela resultado: {err}")
            colunas_resultado = None
        colunas_num = ['NR_TURMA_APURADORA', 'NR_JUNTA_APURADORA', 'QT_VOTOS', 'QT_APTOS', 'QT_COMPARECIMENTO', 'QT_ABSTENCOES']
        colunas_data = ['DT_GERACAO', 'DT_PLEITO']
        colunas_dataHora = ['DT_BU_RECEBIDO','DT_CARGA_URNA_EFETIVADA','DT_ABERTURA','DT_ENCERRAMENTO','DT_EMISSAO_BU']
        colunas_hora = ['HR_GERACAO']
        for bweb in os.listdir(file_csv):
            full_path = os.path.join(file_csv, bweb)
            if full_path.endswith('.csv'):
                df = pd.read_csv(full_path, encoding='latin-1', sep=';',dtype=str,nrows=10)
                for col in colunas_num:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                for col in colunas_data:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                for col in colunas_hora:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.time
                for col in colunas_dataHora:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                df['ID_ZONA_SECAO_MUNICIPIO'] = df['ANO_ELEICAO'] + df['NR_TURNO'] + df['NR_ZONA'] +  df['NR_SECAO'] +  df['CD_MUNICIPIO']
                df['NM_CANDIDATO'] = (
                    df['NM_VOTAVEL']
                    .fillna('')
                    .astype(str)
                    .str.normalize('NFKD')
                    .str.encode('ascii', 'ignore')
                    .str.decode('utf-8')
                    .str.replace(r'[^A-Za-z0-9]+', '', regex=True)
                )
                df['ID_CANDIDATO'] = df['ANO_ELEICAO'] + df['NR_TURNO'] +  df['CD_CARGO_PERGUNTA'] + df['CD_MUNICIPIO'] +  df['NR_VOTAVEL'] + df['NM_CANDIDATO']
                dfs.append(df)
                print(f"Carregando o arquivo no banco de archives: {bweb}")
                df.columns = [col.lower() for col in df.columns]
                if colunas_resultado is not None:
                    colunas_validas = [col for col in df.columns if col in colunas_resultado]
                    colunas_ignoradas = [col for col in df.columns if col not in colunas_resultado]
                    if colunas_ignoradas:
                        print(f"Colunas ignoradas por nao existirem em resultado: {colunas_ignoradas}")
                    if not colunas_validas:
                        print(f"Nenhuma coluna valida para inserir no arquivo: {bweb}")
                        continue
                    df = df[colunas_validas]
                df.to_sql("resultado", engine, index=False, if_exists='append')
                print(f"Arquivo Carregado no Bando de archives: {bweb}")

        return print("Todos os arquivos foram processados e carregados no banco de archives com sucesso!")
    @task
    def unzip_candidato(zip_path, file_save):
        os.makedirs(file_save, exist_ok=True)

        for file_name in os.listdir(zip_path):
            if file_name.lower().endswith('.zip'):
                zip_file = os.path.join(zip_path, file_name)
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    for member in zip_ref.namelist():
                        if member.endswith('/') or '_RR.csv' not in os.path.basename(member):
                            continue
                        nome_arquivo = os.path.basename(member)
                        destino_final = os.path.join(file_save, nome_arquivo)
                        with zip_ref.open(member) as origem, open(destino_final, 'wb') as destino:
                            shutil.copyfileobj(origem, destino)
                        print(f"Extraído: {nome_arquivo}")


    @task
    def candidato(file_csv):

        dfs = []
        lista_links = []
        # Configurações do Cloudflare
        account_id = '8e2ebba9301e868cfdedc13086d6c140'
        access_key = 'a35b4c037c701ade40b95d0c48e4b4b3'
        secret_key = '9eb9f6eac22bb9a13a90f36ab18092e455fef1ead93ec303fe94c0bf2ae90e42'
        bucket_name = 'tre'
        folder_local = os.getenv('CANDIDATO_FOTOS_DIR', '/opt/airflow/TRE2026/archives/RR/candidato/jpg')
        folder_remote = 'fotosrr/'               

        s3 = boto3.client(
        service_name='s3',
        endpoint_url=f'https://{account_id}.r2.cloudflarestorage.com',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='auto', 
        config=Config(signature_version='s3v4')
        )
        base_url = "https://pub-e9ede5cdf96443d58340a9a3b62a2816.r2.dev/fotosrr/" 
        engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@db:5432/eleicao_RR")

        for i in os.listdir(file_csv):
            full_path = os.path.join(file_csv, i)
            if full_path.endswith('RR.csv'):
                print(f"Lendo arquivo: {full_path}")
                df = pd.read_csv(full_path, encoding='latin-1', sep=';',dtype=str)
                df['SG_UE'] = df['SG_UE'].astype(str).str.lstrip("0")
                df.loc[df['NM_UE'] == "RORAIMA", 'SG_UE'] = '3018'
                df['NM_CANDIDATO'] = (
                    df['NM_URNA_CANDIDATO']
                    .fillna('')
                    .astype(str)
                    .str.normalize('NFKD')
                    .str.encode('ascii', 'ignore')
                    .str.decode('utf-8')
                    .str.replace(r'[^A-Za-z0-9]+', '', regex=True)
                )
                df['ID_CANDIDATO'] = df['ANO_ELEICAO'] + df['NR_TURNO'] +  df['CD_CARGO'] + df['SG_UE'] +  df['NR_CANDIDATO'] + df['NM_CANDIDATO']
                dfs.append(df)


        dfconsulta = pd.concat(dfs, ignore_index=True)

        for file in os.listdir(folder_local):
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                lista_links.append({
                    "Nome_Foto": file,
                    "URL_Foto": base_url + file
                })
        fotos = pd.DataFrame(lista_links)
        fotos['idCandidatos'] = fotos['Nome_Foto'].str[2:14]

        df = pd.merge(
        dfconsulta, 
        fotos, 
        how='left', 
        right_on='idCandidatos',
        left_on='SQ_CANDIDATO').drop('idCandidatos', axis=1)
        df.columns = [col.lower() for col in df.columns]
        df.drop_duplicates(['id_candidato'], inplace=True)

        df.to_sql("dcandidato", engine, index=False, if_exists='append')





        return print("Todos os arquivos foram processados e carregados no banco de archives com sucesso!")







### FIM DA FUNÇÃO / CHAMADAS DAS FUNÇÕES PARA CADA TAREFA ###

    unzip_local_votacao_RR = unzip_file.override(task_id="unzip_local_votacao_RR")(
        zip_path = "/opt/airflow/TRE2026/archives/RR/local_votacao/zip",
        file_save="/opt/airflow/TRE2026/archives/RR/local_votacao/csv"
    )
    etl_local_votacao_RR = local_votacao.override(task_id="etl_local_votacao_RR")(path = '/opt/airflow/TRE2026/archives/RR/local_votacao/csv')
    mover_local_votacao_RR = BashOperator(
    task_id='mover_local_votacao_RR',
    bash_command=f'mv /opt/airflow/TRE2026/archives/RR/local_votacao/zip/*.zip /opt/airflow/TRE2026/archives/RR/local_votacao/processados/'
    )


###-------------------------------------------------------------------------------------------------------------------------------------------------#####

    unzip_perfil_eleitor_RR = unzip_file.override(task_id="unzip_perfil_eleitor_RR")(
    zip_path = "/opt/airflow/TRE2026/archives/RR/perfil_eleitorado_secao/zip",
    file_save="/opt/airflow/TRE2026/archives/RR/perfil_eleitorado_secao/csv",
    )
    perfil_eleitorado_RR = perfil.override(task_id="etl_perfil_eleitorado_RR")(file_csv = '/opt/airflow/TRE2026/archives/RR/perfil_eleitorado_secao/csv')
    mover_perfil_eleitorado_RR = BashOperator(
    task_id='mover_perfil_eleitorado_RR',
    bash_command=f'mv /opt/airflow/TRE2026/archives/RR/perfil_eleitorado_secao/zip/*.zip /opt/airflow/TRE2026/archives/RR/perfil_eleitorado_secao/processados/'
    )

# ###-------------------------------------------------------------------------------------------------------------------------------------------------#####
    unzip_resultado_RR = unzip_file.override(task_id="unzip_resultado_RR")(
        zip_path = "/opt/airflow/TRE2026/archives/RR/turnos/zip",
        file_save="/opt/airflow/TRE2026/archives/RR/turnos/csv",
    )
    resultado_RR = turnos.override(task_id="etl_resultado_RR")(file_csv ='/opt/airflow/TRE2026/archives/RR/turnos/csv')
    mover_resultado_RR = BashOperator(
    task_id='mover_resultado_RR',
    bash_command=f'mv /opt/airflow/TRE2026/archives/RR/turnos/zip/*.zip /opt/airflow/TRE2026/archives/RR/turnos/processados/'
    )

###-------------------------------------------------------------------------------------------------------------------------------------------------#####

    unzip_csv_candidato_RR = unzip_candidato.override(task_id="unzip_csv_candidatos_RR")(
        zip_path = "/opt/airflow/TRE2026/archives/RR/candidato/zip-csv",
        file_save="/opt/airflow/TRE2026/archives/RR/candidato/csv"
    )
    unzip_foto_candidato_RR = unzip_file.override(task_id="unzip_foto_candidatos_RR")(
    zip_path = "/opt/airflow/TRE2026/archives/RR/candidato/zip-fotos",
    file_save="/opt/airflow/TRE2026/archives/RR/candidato/jpg"
)
    etl_candidato_RR = candidato.override(task_id="etl_candidato_RR")(file_csv = '/opt/airflow/TRE2026/archives/RR/candidato/csv')

    mover_candidato_csv_RR = BashOperator(
    task_id='mover_candidato_zip_RR',
    bash_command=f'mv /opt/airflow/TRE2026/archives/RR/candidato/zip-csv/*.zip /opt/airflow/TRE2026/archives/RR/candidato/processados-csv/'
    )
    mover_candidato_jpg_RR = BashOperator(
    task_id='mover_candidato_jpg_RR',
    bash_command=f'mv /opt/airflow/TRE2026/archives/RR/candidato/zip-fotos/*.zip /opt/airflow/TRE2026/archives/RR/candidato/processados-jpg/'
    )

#     dbt_eleicao= DbtTaskGroup(
#             group_id="camada_transformacao_dbt",
#             project_config=ProjectConfig(DBT_PROJECT_PATH),
#             profile_config=profile_config,
#             render_config=RenderConfig(
#                 select=["path:models/cinema"]
#             ),
#             operator_args={"install_deps": True},
#         )


    
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')


    start >> unzip_local_votacao_RR  >> etl_local_votacao_RR >> mover_local_votacao_RR >> end
    start >> unzip_perfil_eleitor_RR >> perfil_eleitorado_RR >> mover_perfil_eleitorado_RR >> end
    start >> unzip_resultado_RR >> resultado_RR >> mover_resultado_RR >> end
    start >> [unzip_csv_candidato_RR , unzip_foto_candidato_RR ] >> etl_candidato_RR >> [mover_candidato_csv_RR , mover_candidato_jpg_RR] >> end

eleicao()