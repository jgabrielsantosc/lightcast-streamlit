from .constants import ARRAY_FIELDS, DATE_FIELDS, JOB_FIELDS, NUMERIC_FIELDS
import pandas as pd
from datetime import datetime
import ast
import logging

def prepare_job_data(row):
    """Prepara os dados do job para inserção no Supabase"""
    try:
        # Processar datas
        last_updated_date = pd.to_datetime(row['LAST_UPDATED_DATE']).strftime('%Y-%m-%d') if pd.notna(row['LAST_UPDATED_DATE']) else None
        expired = pd.to_datetime(row['EXPIRED']).strftime('%Y-%m-%d') if pd.notna(row['EXPIRED']) else None
        posted = pd.to_datetime(row['POSTED']).strftime('%Y-%m-%d') if pd.notna(row['POSTED']) else None

        # Processar arrays
        arrays = {
            'SOURCES': [],
            'SKILLS': [],
            'SPECIALIZED_SKILLS': [],
            'SPECIALIZED_SKILLS_NAME': [],
            'COMMON_SKILLS': [],
            'COMMON_SKILLS_NAME': [],
            'SOFTWARE_SKILLS': [],
            'SOFTWARE_SKILLS_NAME': [],
            'CERTIFICATIONS': [],
            'CERTIFICATIONS_NAME': []
        }

        for field in arrays.keys():
            if field in row and pd.notna(row[field]):
                try:
                    arrays[field] = ast.literal_eval(row[field])
                except:
                    arrays[field] = []

        # Processar campos numéricos
        try:
            company = int(row['COMPANY']) if pd.notna(row['COMPANY']) and row['COMPANY'] != '0' and row['COMPANY'] != '' else None
        except:
            company = None

        try:
            max_years = int(float(row['MAX_YEARS_EXPERIENCE'])) if pd.notna(row['MAX_YEARS_EXPERIENCE']) else None
            min_years = int(float(row['MIN_YEARS_EXPERIENCE'])) if pd.notna(row['MIN_YEARS_EXPERIENCE']) else None
        except:
            max_years = None
            min_years = None

        # Montar dados do job
        job_data = {
            "id": row['ID'],
            "last_updated_date": last_updated_date,
            "body": row['BODY'] if pd.notna(row['BODY']) else None,
            "title_raw": row['TITLE_RAW'] if pd.notna(row['TITLE_RAW']) else None,
            "url": row['URL'] if pd.notna(row['URL']) else None,
            "sources": arrays['SOURCES'],
            "language": row['LANGUAGE'] if pd.notna(row['LANGUAGE']) else None,
            "company": company,
            "expired": expired,
            "posted": posted,
            "skills": arrays['SKILLS'],
            "title": row['TITLE'] if pd.notna(row['TITLE']) else None,
            "title_name": row['TITLE_NAME'] if pd.notna(row['TITLE_NAME']) else None,
            "title_clean": row['TITLE_CLEAN'] if pd.notna(row['TITLE_CLEAN']) else None,
            "nation": row['NATION'] if pd.notna(row['NATION']) else None,
            "occupation": row['OCCUPATION'] if pd.notna(row['OCCUPATION']) else None,
            "occupation_name": row['OCCUPATION_NAME'] if pd.notna(row['OCCUPATION_NAME']) else None,
            "specialized_skills": arrays['SPECIALIZED_SKILLS'],
            "specialized_skills_name": arrays['SPECIALIZED_SKILLS_NAME'],
            "common_skills": arrays['COMMON_SKILLS'],
            "common_skills_name": arrays['COMMON_SKILLS_NAME'],
            "software_skills": arrays['SOFTWARE_SKILLS'],
            "software_skills_name": arrays['SOFTWARE_SKILLS_NAME'],
            "certifications": arrays['CERTIFICATIONS'],
            "certifications_name": arrays['CERTIFICATIONS_NAME'],
            "remote_type": row['REMOTE_TYPE'] if pd.notna(row['REMOTE_TYPE']) else None,
            "max_years_experience": max_years,
            "min_years_experience": min_years,
            "last_update_import": datetime.now().isoformat()
        }

        # Remover campos None ou vazios
        return {k: v for k, v in job_data.items() if v is not None and v != ''}

    except Exception as e:
        raise Exception(f"Erro ao preparar dados do job: {str(e)}")

def process_skills(row, supabase):
    """Processa as skills do job"""
    try:
        skills = ast.literal_eval(row['SKILLS']) if pd.notna(row['SKILLS']) else []
        skills_name = ast.literal_eval(row['SKILLS_NAME']) if pd.notna(row['SKILLS_NAME']) else []
        
        # Limpa skills existentes do job
        supabase.table('job_skill').delete().eq('job', row['ID']).execute()
        
        for skill_id, skill_name in zip(skills, skills_name):
            try:
                # Verifica se skill existe
                skill_exists = supabase.table('skill_2_skill_pt_br').select('*').eq('id', skill_id).execute()
                
                if not skill_exists.data:
                    # Cria nova skill
                    skill_data = {
                        "id": skill_id,
                        "name": skill_name,
                        "latest_version": True
                    }
                    supabase.table('skill_2_skill_pt_br').insert(skill_data).execute()
                    print(f"Nova skill criada: {skill_id} - {skill_name}")

                # Vincula skill ao job
                job_skill_data = {
                    "job": row['ID'],
                    "skill": skill_id
                }
                supabase.table('job_skill').insert(job_skill_data).execute()
                
            except Exception as e:
                print(f"Erro ao processar skill {skill_id}: {str(e)}")
                
    except Exception as e:
        print(f"Erro ao processar skills: {str(e)}")
        raise