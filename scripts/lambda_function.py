import boto3
import json

def lambda_handler(event, context):
    glue = boto3.client('glue')
    # Substitua pelo nome exato que você deu no Passo 2
    job_name = 'B3_Job_Transformacao' 
    
    try:
        print(f"Tentando iniciar o job: {job_name}")
        glue.start_job_run(JobName=job_name)
        return {
            'statusCode': 200,
            'body': json.dumps('Job iniciado com sucesso!')
        }
    except Exception as e:
        print(f"Erro: {e}")
        raise e