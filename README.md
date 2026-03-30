# 🎬 Análise do Cinema Nacional (ANCINE)
### Engenharia de Dados & Business Intelligence

![Status](https://img.shields.io/badge/Status-Em_Desenvolvimento-green)
![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.x-red?logo=apache-airflow)
![dbt](https://img.shields.io/badge/dbt-Core-orange?logo=dbt)
![Docker](https://img.shields.io/badge/Docker-Enabled-blue?logo=docker)
![PostgreSQL](https://img.shields.io/badge/Postgres-SQL-blue?logo=postgresql)

---

## 📖 Contexto do Projeto
O que começou como uma curiosidade sobre o cinema local (**Cine Super K**) evoluiu para um projeto de ponta a ponta que analisa o cenário nacional da ANCINE. Como **Economista**, meu foco foi transformar dados brutos governamentais em ativos estratégicos para entender o mercado cinematográfico brasileiro.

Este repositório contém o pipeline completo: desde a ingestão via API/Requests até a modelagem dimensional para consumo em dashboards.

---

## 🛠️ Arquitetura da Solução
A "mágica" por trás dos panos utiliza uma stack moderna de Data Ops:


1.  **Orquestração:** `Apache Airflow` rodando em containers Docker.
2.  **Ingestão:** Scripts `Python` que realizam o download, unzip e limpeza inicial (tipagem e tratamento de nulos).
3.  **Storage:** Armazenamento em banco de dados `PostgreSQL`.
4.  **Transformação:** Utilização de `dbt` para criação de camadas de modelagem (Staging/Marts) e separação por granularidade (Nacional/Estadual).
5.  **Visualização:** Dados prontos para `Power BI`, `Metabase` ou `Apache Superset`.

---

## 📊 Visualização dos Dados
O dashboard final permite filtrar o mercado por estados (como RJ e RR) e analisar o impacto das novas tags de acessibilidade e legendagem.

🔗 **[Acesse o Dashboard no Power BI](https://dub.sh/bilheteriabrasil)**

---

## 📂 Estrutura do Repositório
| Pasta | Descrição |
| :--- | :--- |
| `config/` | Configurações de ambiente e conexões |
| `dags/` | Orquestração do pipeline no Airflow |
| `dbt/` | Modelagem SQL e transformações de dados |
| `jobs/` | Scripts Python de processamento e carga |
| `logs/` | Registros de execução do pipeline |
| `notebooks/` | Análise exploratória e prototipagem |
