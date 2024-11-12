from supabase import create_client
import os
from dotenv import load_dotenv
import pandas as pd

def init_supabase():
    """Inicializa a conexão com o Supabase"""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("Credenciais do Supabase não encontradas no arquivo .env")
    return create_client(url, key)

def verify_company(row, supabase_client):
    """Verifica e cria company se necessário"""
    try:
        company = int(row['COMPANY']) if pd.notna(row['COMPANY']) and row['COMPANY'] != '0' and row['COMPANY'] != '' else None
        if company:
            company_exists = supabase_client.table('company').select('*').eq('id', company).execute()
            if not company_exists.data:
                company_data = {
                    "id": company,
                    "company_name": row['COMPANY_NAME'] if pd.notna(row['COMPANY_NAME']) else ''
                }
                supabase_client.table('company').insert(company_data).execute()
    except Exception as e:
        raise Exception(f"Erro ao verificar company: {str(e)}")

def verify_title(row, supabase_client):
    """Verifica e cria title_taxonomy se necessário"""
    try:
        title = row['TITLE']
        if pd.notna(title) and title != '0' and title != '':
            title_exists = supabase_client.table('title_taxonomy').select('*').eq('id', title).execute()
            if not title_exists.data:
                title_data = {
                    "id": title,
                    "name": row['TITLE_NAME'] if pd.notna(row['TITLE_NAME']) else '',
                    "latest_version": True
                }
                supabase_client.table('title_taxonomy').insert(title_data).execute()
    except Exception as e:
        raise Exception(f"Erro ao verificar title: {str(e)}")