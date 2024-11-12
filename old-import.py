import pandas as pd
from supabase import create_client, Client
import json
import ast
from dotenv import load_dotenv
import os
import datetime

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do Supabase usando variáveis de ambiente
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_KEY")

if not url or not key:
    raise ValueError("As credenciais do Supabase não foram encontradas no arquivo .env")

supabase: Client = create_client(url, key)

def insert_jobs_and_skills(csv_file):
    df = pd.read_csv(csv_file, dtype=str)

    job_count = 0
    max_jobs = 10000000  # Limitar a 10 inserções para teste

    for _, row in df.iterrows():
        if job_count >= max_jobs:
            break

        try:
            # Converter strings de data para o formato correto
            try:
                last_updated_date = pd.to_datetime(row['LAST_UPDATED_DATE']).strftime('%Y-%m-%d') if pd.notna(row['LAST_UPDATED_DATE']) else None
                expired = pd.to_datetime(row['EXPIRED']).strftime('%Y-%m-%d') if pd.notna(row['EXPIRED']) else None
                posted = pd.to_datetime(row['POSTED']).strftime('%Y-%m-%d') if pd.notna(row['POSTED']) else None
            except (ValueError, TypeError):
                last_updated_date = None
                expired = None
                posted = None

            # Converter campos de array
            skills = ast.literal_eval(row['SKILLS']) if row['SKILLS'] else []
            sources = ast.literal_eval(row['SOURCES']) if row['SOURCES'] else []
            specialized_skills = ast.literal_eval(row['SPECIALIZED_SKILLS']) if row['SPECIALIZED_SKILLS'] else []
            specialized_skills_name = ast.literal_eval(row['SPECIALIZED_SKILLS_NAME']) if row['SPECIALIZED_SKILLS_NAME'] else []
            common_skills = ast.literal_eval(row['COMMON_SKILLS']) if row['COMMON_SKILLS'] else []
            common_skills_name = ast.literal_eval(row['COMMON_SKILLS_NAME']) if row['COMMON_SKILLS_NAME'] else []
            software_skills = ast.literal_eval(row['SOFTWARE_SKILLS']) if row['SOFTWARE_SKILLS'] else []
            software_skills_name = ast.literal_eval(row['SOFTWARE_SKILLS_NAME']) if row['SOFTWARE_SKILLS_NAME'] else []
            certifications = ast.literal_eval(row['CERTIFICATIONS']) if row['CERTIFICATIONS'] else []
            certifications_name = ast.literal_eval(row['CERTIFICATIONS_NAME']) if row['CERTIFICATIONS_NAME'] else []

            # Converter company para bigint
            try:
                company = int(row['COMPANY']) if pd.notna(row['COMPANY']) and row['COMPANY'] != '0' and row['COMPANY'] != '' else None
            except (ValueError, TypeError):
                company = None

            # Converter anos de experiência para integer
            try:
                max_years = int(row['MAX_YEARS_EXPERIENCE']) if pd.notna(row['MAX_YEARS_EXPERIENCE']) and row['MAX_YEARS_EXPERIENCE'] != '' else None
                min_years = int(row['MIN_YEARS_EXPERIENCE']) if pd.notna(row['MIN_YEARS_EXPERIENCE']) and row['MIN_YEARS_EXPERIENCE'] != '' else None
            except (ValueError, TypeError):
                max_years = None
                min_years = None

            job_data = {
                "id": row['ID'],
                "last_updated_date": last_updated_date,
                "body": row['BODY'] or None,
                "title_raw": row['TITLE_RAW'] or None,
                "url": row['URL'] or None,
                "sources": sources,
                "language": row['LANGUAGE'] or None,
                "company": company,
                "expired": expired,
                "posted": posted,
                "skills": skills,
                "title": row['TITLE'] or None,
                "title_name": row['TITLE_NAME'] or None,
                "title_clean": row['TITLE_CLEAN'] or None,
                "nation": row['NATION'] or None,
                "occupation": row['OCCUPATION'] or None,
                "occupation_name": row['OCCUPATION_NAME'] or None,
                "specialized_skills": specialized_skills,
                "specialized_skills_name": specialized_skills_name,
                "common_skills": common_skills,
                "common_skills_name": common_skills_name,
                "software_skills": software_skills,
                "software_skills_name": software_skills_name,
                "certifications": certifications,
                "certifications_name": certifications_name,
                "remote_type": row['REMOTE_TYPE'] or None,
                "max_years_experience": max_years,
                "min_years_experience": min_years
            }

            # Garantir que arrays sejam enviados corretamente
            for array_field in ['sources', 'skills', 'specialized_skills', 'specialized_skills_name', 
                                'common_skills', 'common_skills_name', 'software_skills', 
                                'software_skills_name', 'certifications', 'certifications_name']:
                if array_field in job_data and job_data[array_field] is not None:
                    # Garantir que é uma lista válida
                    if isinstance(job_data[array_field], str):
                        try:
                            job_data[array_field] = ast.literal_eval(job_data[array_field])
                        except:
                            job_data[array_field] = []
                    elif not isinstance(job_data[array_field], list):
                        job_data[array_field] = []

            # Remover campos None ou vazios
            job_data = {k: v for k, v in job_data.items() if v is not None and v != ''}

            # Debug: Imprimir os dados antes de enviar
            print("\nDados que serão enviados:")
            print(f"ID: {job_data['id']}")
            print(f"Company: {job_data['company']}")
            print(f"Title: {job_data['title']}")
            print(f"Skills: {job_data['skills']}")
            
            # Verificar se há campos vazios ou None
            empty_fields = {k: v for k, v in job_data.items() if v is None or v == ''}
            if empty_fields:
                print("\nCampos vazios ou None:")
                for field, value in empty_fields.items():
                    print(f"{field}: {value}")

            # Verificar se o job_data não está vazio
            if not job_data['id']:
                print("ID do job está vazio, pulando inserção.")
                continue

            # Verificar/criar company se não existir
            if company:
                company_exists = supabase.table('company').select('*').eq('id', company).execute()
                if not company_exists.data:
                    company_data = {
                        "id": company,
                        "company_name": row['COMPANY_NAME']
                    }
                    supabase.table('company').insert(company_data).execute()
                    print(f"Nova empresa criada: {company}")

            # Verificar se o job já existe
            existing_job = supabase.table('jobs').select('*').eq('id', row['ID']).execute()
            
            if existing_job.data:
                # Atualizar job existente
                job_data['last_update_import'] = datetime.datetime.now().isoformat()
                response = supabase.table('jobs').update(job_data).eq('id', row['ID']).execute()
                print(f"Job atualizado: {row['ID']}")
            else:
                # Inserir novo job
                job_data['last_update_import'] = datetime.datetime.now().isoformat()
                response = supabase.table('jobs').insert(job_data).execute()
                print(f"Novo job inserido: {row['ID']}")

            # Processar skills
            for skill_id, skill_name in zip(skills, ast.literal_eval(row['SKILLS_NAME'])):
                skill_exists = supabase.table('skill_2_skill_pt_br').select('*').eq('id', skill_id).execute()
                
                if not skill_exists.data:
                    # Criar nova skill com ID e nome
                    skill_data = {
                        "id": skill_id,
                        "name": skill_name,  # Adicionando o nome da skill

                        "latest_version": True
                    }
                    supabase.table('skill_2_skill_pt_br').insert(skill_data).execute()
                    print(f"Nova skill criada: {skill_id} - {skill_name}")

                # Vincular skill ao job
                job_skill_data = {
                    "job": row['ID'],
                    "skill": skill_id
                }
                
                try:
                    supabase.table('job_skill').insert(job_skill_data).execute()
                except Exception as e:
                    print(f"Erro ao vincular skill {skill_id} ao job {row['ID']}: {e}")

            job_count += 1

        except Exception as e:
            print(f"Erro ao processar linha: {e}")
            print(f"Dados que causaram erro: {job_data}")

# Uso
insert_jobs_and_skills("all-jobs-11-11-2024.csv")