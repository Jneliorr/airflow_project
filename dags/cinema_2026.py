from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param, ParamsDict
from sqlalchemy import create_engine, VARCHAR, Integer,Date
import os
import pandas as pd
from datetime import datetime
import zipfile
import numpy as np
from airflow.operators.bash import BashOperator

params = {
        "Usuario": Param(
        default="José Nélio"
        ,type='string'
        ,enum=["José Nélio"]
        ,description="""
        Escolher usuario 
        """
        )
    ,'anos': Param(
            default='2026'
            ,type="string"
            ,description="Escolher ano ou Todos ")
    ,'estados': Param(
            default='RR'
            ,type="string"
            ,description="Estado Filtrado")
    ,'table_name': Param(
        default='cinema_BR'
        ,type="string"
        ,description="Nome da Tabela para subir ao postgres")
    ,'database': Param(
        default='mydatabase'
        ,type="string"
        ,description="Nome da database para subir ao postgres")
    ,'user': Param(
        default='postgres'
        ,type="string"
        ,description="Nome do user para subir ao postgres")
    ,'password': Param(
        default='postgres'
        ,type="string"
        ,description="Password para subir ao postgres")
    }

default_args = {
    "owner": "Nelio Cruel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dag_cinema",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    params=params,
    catchup=False,
    tags=["cinema"],
    max_active_tasks= 30,
    default_args=default_args
)


def cinema2026():
    
    @task
    def unzip_file(zip_path, file_save, anos=""):

        zip_name = os.path.splitext(os.path.basename(zip_path))[0]
        extract_to = os.path.join(file_save, zip_name)
        os.makedirs(extract_to, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if anos == 'TODOS' or str(anos) in file_name:
                    zip_ref.extract(file_name, extract_to)
                    print(f"Extraído: {file_name}")

    @task
    def read_bilheteria(caminho_arquivo,anos,estados, row, caminho_saida):
        # nome_arquivo_saida = os.path.join(caminho_saida, "bilheteria_diaria_tratada.csv")
        # header_written = False

        # for arquivo in os.listdir(caminho_arquivo):
        #     if arquivo.endswith('.csv') and (anos == 'TODOS' or anos in arquivo):
        #         caminho_completo = os.path.join(caminho_arquivo, arquivo)
        #         print(f"Lendo: {caminho_completo}")

        #         for chunk in pd.read_csv(caminho_completo, delimiter=';', dtype=str, chunksize=50000):
        #             chunk['NOME_ARQUIVO'] = arquivo

        #             if estados and estados != "TODOS":
        #                 chunk = chunk[chunk['UF_SALA_COMPLEXO'] == estados]

        #             chunk['TITULO_FILME'] = chunk.apply(
        #                 lambda row: row['TITULO_BRASIL'] if pd.notnull(row['TITULO_BRASIL']) else row['TITULO_ORIGINAL'],
        #                 axis=1
        #             )
        #             chunk.insert(1, 'TITULO_FILME', chunk.pop('TITULO_FILME'))
        #             chunk = chunk[chunk['REGISTRO_SALA'].notnull()]

        #             # Grava direto no CSV sem acumular na RAM
        #             chunk.to_csv(nome_arquivo_saida, sep=';', index=False, encoding='utf-8',
        #                         mode='a', header=not header_written)
        #             header_written = True

        #         print(f"Lido: {caminho_completo}")

        # print(f"Salvo com sucesso em: {nome_arquivo_saida}")       
        dataframe = []
        for arquivo in os.listdir(caminho_arquivo):
            if arquivo.endswith('.csv') and anos:
                ano_encontrado = anos in arquivo
                if anos == 'TODOS' or ano_encontrado:
                    caminho_completo = os.path.join(caminho_arquivo, arquivo)
                    print(f"lendo: {caminho_completo}")
                    df = pd.read_csv(caminho_completo, delimiter=';', dtype=str)
                    df['NOME_ARQUIVO'] = arquivo
                    dataframe.append(df)
                    print(f"lido: {caminho_completo}")
        df_final = pd.concat(dataframe, ignore_index=True)
        print("passou concat")
        if estados and estados != "TODOS":
            df_final = df_final[df_final['UF_SALA_COMPLEXO'] == estados]
        df_final['TITULO_FILME'] = df_final.apply(lambda row: row['TITULO_BRASIL'] 
                                                if pd.notnull(row['TITULO_BRASIL'])
                                                else row['TITULO_ORIGINAL'], axis=1)
        df_final.insert(1, 'TITULO_FILME', df_final.pop('TITULO_FILME'))
        df_final = df_final[df_final['REGISTRO_SALA'].notnull()]
        nome_arquivo_saida = os.path.join(caminho_saida, "bilheteria_diaria_tratada.csv")
        df_final.to_csv(nome_arquivo_saida, sep=';', index=False, encoding='utf-8')
        print(f"Salvo com sucesso em: {nome_arquivo_saida}")


    @task
    def d_cinemas_salas(caminho, caminho_saida, estados):
        df_sala = pd.read_csv(caminho, delimiter=';', dtype=str)
        if estados:
            df_sala = df_sala[df_sala['UF_COMPLEXO'] == estados]
        # df_sala = df_sala[df_sala['UF_COMPLEXO'] == estados]
        nome_arquivo_saida = os.path.join(caminho_saida, "d_cinema.csv")
        df_sala.to_csv(nome_arquivo_saida, sep=';', index=False, encoding='utf-8')
        print(f"d_cinemas salvo com sucesso em: {nome_arquivo_saida}")

    @task
    def d_filmes(caminho, colunas,caminho_saida):
        df_filmes = pd.read_csv(caminho, delimiter=';',dtype=str, usecols=colunas)
        df_filmes = df_filmes.drop_duplicates(subset=['CPB_ROE'])
        nome_arquivo_saida = os.path.join(caminho_saida, "d_filmes.csv")
        df_filmes.to_csv(nome_arquivo_saida, sep=';',index=False, encoding='utf-8')
        print(f"d_filmes salvo com sucesso em: {nome_arquivo_saida}")

    @task
    def lancamentos (caminhodist,caminho_saida):
        lancamento = pd.read_csv(caminhodist, delimiter=';',dtype=str)
        lancamento['PAIS_OBRA'] = np.where(lancamento['PAIS_OBRA'] == 'BRASIL', 'NACIONAL', 'ESTRANGEIRO')
        nome_arquivo_saida = os.path.join(caminho_saida, "lancamentos.csv")
        lancamento.to_csv(nome_arquivo_saida, sep=';',index=False, encoding='utf-8')

    # @task
    # def upload_to_postgres(caminho, table_name, database, user, password):
    #     # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    #     engine = create_engine(f"postgresql+psycopg2://{user}:{password}@172.17.0.2:5432/{database}")
    #     print(engine)
    #     df = pd.read_csv(caminho, sep=';')
    #     df.to_sql(table_name, engine, index=False, if_exists='replace')


#### FIM DA FUNÇÃO / CHAMADAS DAS FUNÇÕES PARA CADA TAREFA ###

    unzip = unzip_file(
        zip_path = "/opt/airflow/cinema2026/data/raw/zip/bilheteria-diaria-obras-por-exibidoras-csv.zip",
        file_save="/opt/airflow/cinema2026/data/raw/unzip",
        anos = "{{ params.anos }}"
    )

    etl =  read_bilheteria(caminho_arquivo = "/opt/airflow/cinema2026/data/raw/unzip/bilheteria-diaria-obras-por-exibidoras-csv",
                anos = "{{ params.anos }}",
                estados = "{{ params.estados }}",
                row= None,
                caminho_saida = "/opt/airflow/cinema2026/data/processados"
                )
    
    cinemas = d_cinemas_salas(caminho= "/opt/airflow/cinema2026/data/raw/unzip/salas-de-exibicao/salas-de-exibicao-e-complexos.csv", 
                caminho_saida = "/opt/airflow/cinema2026/data/processados", 
                estados = "{{ params.estados }}")
    
    filmes = d_filmes("/opt/airflow/cinema2026/data/processados/bilheteria_diaria_tratada.csv",
         ['CPB_ROE', 'TITULO_FILME', 'TITULO_ORIGINAL', 'TITULO_BRASIL', 'PAIS_OBRA'],
         "/opt/airflow/cinema2026/data/processados")
    
    distribuidoras = lancamentos("/opt/airflow/cinema2026/data/raw/unzip/distribuidoras/lancamentos-comerciais-por-distribuidoras.csv",
         "/opt/airflow/cinema2026/data/processados")
    
    mover_zips = BashOperator(
    task_id='mover_zips',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/zip/bilheteria-diaria-obras-por-exibidoras-csv.zip /opt/airflow/cinema2026/data/raw/arquivo/zip/bilheteria-diaria-obras-por-exibidoras/ '
    )
    mover_read_bilheteria = BashOperator(
    task_id='mover_read_bilheteria',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/bilheteria-diaria-obras-por-exibidoras-csv/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/bilheteria-diaria-obras-por-exibidoras-csv/'
    )

    mover_read_distribuidoras = BashOperator(
    task_id='mover_read_distribuidoras',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/distribuidoras/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/distribuidoras/'
    )
    mover_salas_exibicao = BashOperator(
    task_id='mover_salas_exibicao',
    bash_command=f'mv /opt/airflow/cinema2026/data/raw/unzip/salas-de-exibicao/*.csv /opt/airflow/cinema2026/data/raw/arquivo/unzip/salas-de-exibicao/ '
    )

    # upload = upload_to_postgres(caminho = "/opt/airflow/cinema2026/data/processados/bilheteria_diaria_tratada.csv", table_name = "{{ params.table_name }}", database = "{{ params.database }}", user = "{{ params.user }}", password = "{{ params.password }}")
    
    
    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id = 'end')

    start >> unzip >> etl >> cinemas >> filmes >> distribuidoras >> [mover_zips, mover_read_bilheteria, mover_read_distribuidoras, mover_salas_exibicao ] >> end

cinema2026()