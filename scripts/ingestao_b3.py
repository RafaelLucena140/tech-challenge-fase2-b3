import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime
import io
import os
import shutil

# --- CONFIGURAÇÕES ---
TICKERS = ["PETR4.SA", "ITUB4.SA", "VALE3.SA"]
BUCKET_NAME = "fiap-b3-bucket-rafael"
REGION = "us-east-2"
FOLDER_NAME = "raw"

def limpar_cache_yfinance():
    try:
        cache_path = os.path.join(os.environ.get('LOCALAPPDATA'), 'py-yfinance')
        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)
            print(f"Cache limpo: {cache_path}")
    except Exception:
        pass

def ingest_b3_data():
    limpar_cache_yfinance()

    try:
        print("Iniciando extração...")
        data = yf.download(TICKERS, period="1d", interval="1d")
        
        if data.empty:
            print("Nenhum dado encontrado.")
            return

        # Stack para transformar em formato longo
        df = data.stack(level=1, future_stack=True).reset_index()
        
        # Adiciona data de processamento
        df['dt_processamento'] = datetime.now()

        # Preparação
        execution_date = datetime.now().strftime("%Y-%m-%d")
        parquet_buffer = io.BytesIO()
        
        # coerce_timestamps='us' converte nanosegundos para microsegundos, aceitos pelo Glue
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', coerce_timestamps='us')
        
        s3_key = f"{FOLDER_NAME}/data={execution_date}/b3_data.parquet"
        
        print(f"Enviando para S3: {s3_key}...")
        
        s3_client = boto3.client('s3', region_name=REGION)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )
        
        print("\n✅ Arquivo enviado com a correção de data!")

    except Exception as e:
        print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    ingest_b3_data()