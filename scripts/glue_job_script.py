import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # <--- Importante: Adicionei este import
from pyspark.sql.functions import col, avg, date_format
from pyspark.sql.window import Window

# --- CONFIGURAÇÕES INICIAIS ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Definição do Bucket
BUCKET_NAME = "fiap-b3-bucket-rafael"

# 1. LEITURA (RAW)
datasource0 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [f"s3://{BUCKET_NAME}/raw/"], "recurse": True},
    transformation_ctx="datasource0"
)

# Convertendo para DataFrame Spark
df = datasource0.toDF()

# --- REQUISITO 5: TRANSFORMAÇÕES ---

# (B) Renomear colunas (Garante que só renomeia se a coluna existir)
if "Close" in df.columns:
    df = df.withColumnRenamed("Close", "Valor_Fechamento")
if "Volume" in df.columns:
    df = df.withColumnRenamed("Volume", "Volume_Negociado")

# (C) Média Móvel de 7 dias
if "Date" in df.columns and "Valor_Fechamento" in df.columns:
    windowSpec = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-6, 0)
    df = df.withColumn("Media_Movel_7d", avg("Valor_Fechamento").over(windowSpec))

# (A) Agrupamento Mensal
df = df.withColumn("Mes_Ano", date_format(col("Date"), "yyyy-MM"))
windowAgrupamento = Window.partitionBy("Ticker", "Mes_Ano")
df = df.withColumn("Media_Mensal", avg("Valor_Fechamento").over(windowAgrupamento))

# --- CORREÇÃO AQUI ---
# Convertendo de volta para DynamicFrame da forma correta
dyf_refined = DynamicFrame.fromDF(df, glueContext, "dyf_refined")

# --- REQUISITOS 6 e 7: SALVAR E CATALOGAR ---
try:
    sink = glueContext.getSink(
        path=f"s3://{BUCKET_NAME}/refined/",
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["Date", "Ticker"], 
        enableUpdateCatalog=True,
        transformation_ctx="sink"
    )
    sink.setCatalogInfo(catalogDatabase="default", catalogTableName="b3_refined_data")
    sink.setFormat("glueparquet")
    sink.writeFrame(dyf_refined)
    print("Sucesso! Dados salvos e catalogados.")
except Exception as e:
    print(f"Erro ao salvar: {e}")
    raise e

job.commit()