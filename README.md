🎬 Análise do Cinema Nacional (ANCINE) - Pipeline End-to-End
📖 Sobre o Projeto
Este projeto nasceu da evolução de uma curiosidade acadêmica sobre a bilheteria local (o antigo "Cine Super K") para uma solução robusta de engenharia de dados em escala nacional. O objetivo é extrair, tratar e visualizar dados públicos da ANCINE (GovBr), permitindo análises granulares sobre o consumo cinematográfico no Brasil.

O pipeline lida com desafios reais, como mudanças na estrutura dos dados governamentais e a recente inclusão de indicadores de acessibilidade e legendagem.

🛠️ Stack Tecnológica
Orquestração: Apache Airflow (Maestro do fluxo).

Ingestão: Python (Requests & Pandas para limpeza pesada).

Containerização: Docker & Docker Compose.

Data Warehouse: PostgreSQL.

Transformação: dbt (Data Build Tool) para modelagem de camadas.

Visualização: Power BI / Metabase / Apache Superset.

🏗️ Arquitetura e Fluxo de Dados
Ingestão: Scripts Python automatizados buscam arquivos brutos (CSV/Excel) diretamente do portal GovBr.

Processamento (Bronze): Limpeza de nulos, tipagem de datas e conversão de valores monetários.

Storage: Carga dos dados tratados no PostgreSQL.

Modelagem dbt (Silver/Gold): * Criação de visualizações estratégicas por estado (Ex: RJ, RR).

Separação de métricas de bilheteria vs. público.

Manutenção: Movimentação de arquivos processados para pastas de histórico (logs e backups).

📊 Visualização
O resultado final pode ser explorado através do dashboard interativo:
🔗 Acesse o Dashboard no Power BI aqui

🚀 Como Executar
Certifique-se de ter o Docker instalado e execute:

Bash
# Clonar o repositório
git clone https://github.com/Jneliorr/seu-repositorio.git

# Subir o ambiente (Airflow + Postgres)
docker-compose up -d
As DAGs estarão disponíveis em localhost:8080.

📂 Estrutura do Repositório
dags/: Definições dos fluxos do Airflow.

dbt/: Modelos de transformação SQL.

jobs/: Scripts Python de processamento inicial.

notebooks/: Análises exploratórias (EDA).

config/: Arquivos de configuração de ambiente.
