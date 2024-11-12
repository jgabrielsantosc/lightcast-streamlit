import streamlit as st
# Configuração da página deve ser a primeira chamada Streamlit
st.set_page_config(page_title="Importador de Jobs", layout="wide")

import pandas as pd
from supabase import create_client, Client
import json
import ast
from dotenv import load_dotenv
import os
import datetime
import time

# Configurações globais
BATCH_SIZE = 10

# Inicialização do Supabase
@st.cache_resource
def init_connection():
    """Inicializa a conexão com o Supabase"""
    try:
        load_dotenv()
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            st.error("Credenciais do Supabase não encontradas no arquivo .env")
            st.stop()
            
        return create_client(url, key)
    except Exception as e:
        st.error(f"Erro ao configurar conexão com Supabase: {str(e)}")
        st.stop()

# Inicializa o cliente Supabase
supabase = init_connection()

def process_skills(row, supabase_client):
    """Processa as skills do job"""
    try:
        if not pd.notna(row['SKILLS']) or not pd.notna(row['SKILLS_NAME']):
            return
            
        skills = ast.literal_eval(row['SKILLS'])
        skills_names = ast.literal_eval(row['SKILLS_NAME'])
        
        for skill_id, skill_name in zip(skills, skills_names):
            # Verifica se a skill já existe
            skill_exists = supabase_client.table('skill_2_skill_pt_br').select('*').eq('id', skill_id).execute()
            
            if not skill_exists.data:
                skill_data = {
                    "id": skill_id,
                    "name": skill_name,
                    "latest_version": True
                }
                supabase_client.table('skill_2_skill_pt_br').insert(skill_data).execute()
            
            # Vincula skill ao job
            job_skill_data = {
                "job": row['ID'],
                "skill": skill_id
            }
            supabase_client.table('job_skill').insert(job_skill_data).execute()
                
    except Exception as e:
        raise Exception(f"Erro ao processar skills: {str(e)}")

def prepare_job_data(row):
    """Prepara os dados do job para inserção/atualização"""
    try:
        # Converter datas com tratamento de erro
        try:
            last_updated_date = pd.to_datetime(row['LAST_UPDATED_DATE']).strftime('%Y-%m-%d') if pd.notna(row['LAST_UPDATED_DATE']) else None
            expired = pd.to_datetime(row['EXPIRED']).strftime('%Y-%m-%d') if pd.notna(row['EXPIRED']) else None
            posted = pd.to_datetime(row['POSTED']).strftime('%Y-%m-%d') if pd.notna(row['POSTED']) else None
        except (ValueError, TypeError):
            last_updated_date = None
            expired = None
            posted = None

        # Converter arrays com tratamento de erro
        try:
            skills = ast.literal_eval(row['SKILLS']) if pd.notna(row['SKILLS']) else []
            sources = ast.literal_eval(row['SOURCES']) if pd.notna(row['SOURCES']) else []
            specialized_skills = ast.literal_eval(row['SPECIALIZED_SKILLS']) if pd.notna(row['SPECIALIZED_SKILLS']) else []
            specialized_skills_name = ast.literal_eval(row['SPECIALIZED_SKILLS_NAME']) if pd.notna(row['SPECIALIZED_SKILLS_NAME']) else []
            common_skills = ast.literal_eval(row['COMMON_SKILLS']) if pd.notna(row['COMMON_SKILLS']) else []
            common_skills_name = ast.literal_eval(row['COMMON_SKILLS_NAME']) if pd.notna(row['COMMON_SKILLS_NAME']) else []
            software_skills = ast.literal_eval(row['SOFTWARE_SKILLS']) if pd.notna(row['SOFTWARE_SKILLS']) else []
            software_skills_name = ast.literal_eval(row['SOFTWARE_SKILLS_NAME']) if pd.notna(row['SOFTWARE_SKILLS_NAME']) else []
            certifications = ast.literal_eval(row['CERTIFICATIONS']) if pd.notna(row['CERTIFICATIONS']) else []
            certifications_name = ast.literal_eval(row['CERTIFICATIONS_NAME']) if pd.notna(row['CERTIFICATIONS_NAME']) else []
        except (ValueError, SyntaxError):
            skills = []
            sources = []
            specialized_skills = []
            specialized_skills_name = []
            common_skills = []
            common_skills_name = []
            software_skills = []
            software_skills_name = []
            certifications = []
            certifications_name = []

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
            "body": row['BODY'] if pd.notna(row['BODY']) else None,
            "title_raw": row['TITLE_RAW'] if pd.notna(row['TITLE_RAW']) else None,
            "url": row['URL'] if pd.notna(row['URL']) else None,
            "sources": sources,
            "language": row['LANGUAGE'] if pd.notna(row['LANGUAGE']) else None,
            "company": company,
            "company_name": row['COMPANY_NAME'] if pd.notna(row['COMPANY_NAME']) else None,
            "expired": expired,
            "posted": posted,
            "skills": skills,
            "title": row['TITLE'] if pd.notna(row['TITLE']) else None,
            "title_name": row['TITLE_NAME'] if pd.notna(row['TITLE_NAME']) else None,
            "title_clean": row['TITLE_CLEAN'] if pd.notna(row['TITLE_CLEAN']) else None,
            "nation": row['NATION'] if pd.notna(row['NATION']) else None,
            "occupation": row['OCCUPATION'] if pd.notna(row['OCCUPATION']) else None,
            "occupation_name": row['OCCUPATION_NAME'] if pd.notna(row['OCCUPATION_NAME']) else None,
            "specialized_skills": specialized_skills,
            "specialized_skills_name": specialized_skills_name,
            "common_skills": common_skills,
            "common_skills_name": common_skills_name,
            "software_skills": software_skills,
            "software_skills_name": software_skills_name,
            "certifications": certifications,
            "certifications_name": certifications_name,
            "remote_type": row['REMOTE_TYPE'] if pd.notna(row['REMOTE_TYPE']) else None,
            "max_years_experience": max_years,
            "min_years_experience": min_years,
            "last_update_import": datetime.datetime.now().isoformat()
        }

        # Remover campos None ou vazios
        job_data = {k: v for k, v in job_data.items() if v is not None and v != ''}

        # Garantir que arrays sejam enviados corretamente
        for array_field in ['sources', 'skills', 'specialized_skills', 'specialized_skills_name', 
                          'common_skills', 'common_skills_name', 'software_skills', 
                          'software_skills_name', 'certifications', 'certifications_name']:
            if array_field in job_data and job_data[array_field] is not None:
                if isinstance(job_data[array_field], str):
                    try:
                        job_data[array_field] = ast.literal_eval(job_data[array_field])
                    except:
                        job_data[array_field] = []
                elif not isinstance(job_data[array_field], list):
                    job_data[array_field] = []

        return job_data

    except Exception as e:
        raise Exception(f"Erro ao preparar dados do job: {str(e)}")

def verify_company(row, supabase_client):
    """Verifica e cria company se necessário"""
    try:
        company = int(row['COMPANY']) if pd.notna(row['COMPANY']) and row['COMPANY'] != '0' and row['COMPANY'] != '' else None
        
        if company:
            company_exists = supabase_client.table('company').select('*').eq('id', company).execute()
            if not company_exists.data:
                company_data = {
                    "id": company,
                    "company_name": row['COMPANY_NAME']
                }
                supabase_client.table('company').insert(company_data).execute()
    except Exception as e:
        raise Exception(f"Erro ao verificar company: {str(e)}")

# Interface Streamlit
st.title("Importador de Jobs para Supabase")

# Upload do arquivo
uploaded_file = st.file_uploader("Escolha o arquivo CSV", type="csv")

if uploaded_file is not None:
    try:
        # Lê o CSV
        df = pd.read_csv(uploaded_file, dtype=str)
        total_rows = len(df)
        
        # Calcula número total de lotes
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        
        # Interface de processamento
        if st.button("Iniciar Processamento", key="start_processing"):
            with st.container():
                # Barra de progresso e status
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Métricas em colunas
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    total_metric = st.metric("Total", total_rows)
                with col2:
                    processed_metric = st.metric("Processados", 0)
                with col3:
                    updated_metric = st.metric("Atualizados", 0)
                with col4:
                    new_metric = st.metric("Novos", 0)
                
                # Área de log
                log_container = st.empty()
                error_container = st.empty()
                
                # Processamento em lotes
                jobs_processados = 0
                jobs_atualizados = 0
                jobs_novos = 0
                errors = []
                
                # Processa cada lote
                for batch_start in range(0, total_rows, BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, total_rows)
                    batch_df = df.iloc[batch_start:batch_end]
                    
                    status_text.text(f"Processando lote {batch_start//BATCH_SIZE + 1} de {total_batches}")
                    
                    for index, row in batch_df.iterrows():
                        try:
                            # Verifica se o job já existe
                            existing_job = supabase.table('jobs').select('*').eq('id', row['ID']).execute()
                            
                            # Prepara os dados do job
                            job_data = prepare_job_data(row)
                            
                            # Dentro do loop de processamento, antes de inserir/atualizar o job
                            verify_company(row, supabase)
                            
                            if existing_job.data:
                                # Atualiza job existente
                                supabase.table('jobs').update(job_data).eq('id', row['ID']).execute()
                                jobs_atualizados += 1
                            else:
                                # Insere novo job
                                supabase.table('jobs').insert(job_data).execute()
                                jobs_novos += 1
                            
                            # Processa skills
                            process_skills(row, supabase)
                            
                            jobs_processados += 1
                            
                            # Atualiza métricas
                            progress = (jobs_processados) / total_rows
                            progress_bar.progress(progress)
                            processed_metric.metric("Processados", jobs_processados)
                            updated_metric.metric("Atualizados", jobs_atualizados)
                            new_metric.metric("Novos", jobs_novos)
                            
                            # Atualiza log
                            log_container.text(
                                f"Processando: {jobs_processados}/{total_rows} ({progress:.1%})\n"
                                f"Atualizados: {jobs_atualizados} | Novos: {jobs_novos}"
                            )
                            
                        except Exception as e:
                            errors.append(f"Erro na linha {index}: {str(e)}")
                            error_container.error("\n".join(errors[-5:]))
                    
                    # Pequena pausa entre lotes
                    time.sleep(0.1)
                
                # Finalização
                if jobs_processados == total_rows:
                    st.success(f"""
                        Processamento concluído com sucesso!
                        - Total processado: {jobs_processados:,} jobs
                        - Atualizados: {jobs_atualizados:,}
                        - Novos: {jobs_novos:,}
                        - Erros: {len(errors)}
                    """)
                else:
                    st.warning(f"""
                        Processamento concluído com alertas!
                        - Total processado: {jobs_processados:,} de {total_rows:,} jobs
                        - Atualizados: {jobs_atualizados:,}
                        - Novos: {jobs_novos:,}
                        - Erros: {len(errors)}
                    """)
                
                # Exibe todos os erros se houver
                if errors:
                    with st.expander("Ver todos os erros"):
                        st.text("\n".join(errors))
                    
    except Exception as e:
        st.error(f"Erro ao processar arquivo: {str(e)}")
        st.stop()