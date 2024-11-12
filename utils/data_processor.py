import pandas as pd
import ast
from datetime import datetime

def prepare_job_data(row):
    """Prepara os dados do job para inserção/atualização"""
    try:
        # Converter datas
        date_fields = ['LAST_UPDATED_DATE', 'EXPIRED', 'POSTED']
        dates = {}
        for field in date_fields:
            try:
                dates[field.lower()] = pd.to_datetime(row[field]).strftime('%Y-%m-%d') if pd.notna(row[field]) else None
            except (ValueError, TypeError):
                dates[field.lower()] = None

        # Converter arrays
        array_fields = {
            'SKILLS': 'skills',
            'SOURCES': 'sources',
            'SPECIALIZED_SKILLS': 'specialized_skills',
            'SPECIALIZED_SKILLS_NAME': 'specialized_skills_name',
            'COMMON_SKILLS': 'common_skills',
            'COMMON_SKILLS_NAME': 'common_skills_name',
            'SOFTWARE_SKILLS': 'software_skills',
            'SOFTWARE_SKILLS_NAME': 'software_skills_name',
            'CERTIFICATIONS': 'certifications',
            'CERTIFICATIONS_NAME': 'certifications_name'
        }
        
        arrays = {}
        for field, key in array_fields.items():
            try:
                arrays[key] = ast.literal_eval(row[field]) if pd.notna(row[field]) else []
            except (ValueError, SyntaxError):
                arrays[key] = []

        job_data = {
            "id": row['ID'],
            "last_updated_date": dates['last_updated_date'],
            "expired": dates['expired'],
            "posted": dates['posted'],
            "body": row['BODY'] if pd.notna(row['BODY']) else None,
            "title": row['TITLE'] if pd.notna(row['TITLE']) else None,
            "company": int(row['COMPANY']) if pd.notna(row['COMPANY']) and row['COMPANY'] != '0' else None,
            **arrays,
            "last_update_import": datetime.now().isoformat()
        }

        return {k: v for k, v in job_data.items() if v is not None and v != ''}

    except Exception as e:
        raise Exception(f"Erro ao preparar dados do job: {str(e)}")

def process_skills(row, supabase):
    """Processa as skills do job"""
    try:
        skills = ast.literal_eval(row['SKILLS']) if pd.notna(row['SKILLS']) else []
        skills_names = ast.literal_eval(row['SKILLS_NAME']) if pd.notna(row['SKILLS_NAME']) else []
        
        for skill_id, skill_name in zip(skills, skills_names):
            try:
                skill_exists = supabase.table('skill_2_skill_pt_br').select('*').eq('id', skill_id).execute()
                
                if not skill_exists.data:
                    skill_data = {
                        "id": skill_id,
                        "name": skill_name,
                        "latest_version": True
                    }
                    supabase.table('skill_2_skill_pt_br').insert(skill_data).execute()

                job_skill_data = {
                    "job": row['ID'],
                    "skill": skill_id
                }
                supabase.table('job_skill').insert(job_skill_data).execute()
                
            except Exception as e:
                raise Exception(f"Erro ao processar skill {skill_id}: {str(e)}")
                
    except Exception as e:
        raise Exception(f"Erro ao processar skills: {str(e)}")