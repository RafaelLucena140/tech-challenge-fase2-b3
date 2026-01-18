# 📊 Tech Challenge - Pipeline de Engenharia de Dados (B3)

Este projeto compõe a entrega da **Fase 2** da Pós-Tech em Data Analytics. O objetivo foi construir um pipeline de dados completo na nuvem AWS para a ingestão, processamento e análise de dados da Bolsa de Valores (B3).

## 🚀 Arquitetura da Solução

O pipeline foi desenhado para ser **Event-Driven** (orientado a eventos), garantindo automação total desde a ingestão até a disponibilização dos dados.

**Fluxo de Dados:**
1.  **Ingestão (Local):** Script Python extrai dados de ações (PETR4, VALE3, ITUB4) via `yfinance`.
2.  **Armazenamento Raw (S3):** Os dados brutos são enviados para a camada `raw/` no Amazon S3 em formato Parquet.
3.  **Automação (Lambda):** O upload no S3 aciona automaticamente uma função **AWS Lambda**.
4.  **Processamento (Glue):** A Lambda inicia um Job do **AWS Glue (Spark)**.
5.  **Transformação:** O Glue lê os dados, limpa, calcula a média móvel de 7 dias e renomeia colunas.
6.  **Armazenamento Refined (S3):** Os dados processados são salvos na camada `refined/` particionados por Data e Ticker.
7.  **Analytics (Athena):** Os dados são catalogados e ficam disponíveis para consulta SQL no **AWS Athena**.

---

## 🛠️ Tecnologias Utilizadas

* **Linguagem:** Python 3.9+
* **Bibliotecas:** `boto3`, `pandas`, `yfinance`, `pyarrow`
* **AWS Services:**
    * **S3:** Data Lake (Camadas Raw e Refined).
    * **Lambda:** Gatilho de eventos (Trigger).
    * **Glue:** ETL Serverless com PySpark.
    * **Athena:** Consultas Ad-hoc (SQL).
    * **IAM:** Gestão de permissões e segurança.

---

## 📂 Estrutura do Projeto

```text
/
├── scripts/
│   ├── ingestao_b3.py        # Script local para extração e envio ao S3
│   ├── lambda_function.py    # Código da função Lambda (Trigger)
│   └── glue_job_script.py    # Script PySpark executado no AWS Glue
├── requirements.txt          # Dependências do projeto
└── README.md                 # Documentação