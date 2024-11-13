import streamlit as st
import pandas as pd
from utils.data_processor import prepare_job_data, process_skills
from utils.supabase_handler import init_supabase, verify_company, verify_title, insert_or_update_job
import time
from datetime import datetime

# Configuração da página
st.set_page_config(
    page_title="Importador de Jobs",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache da conexão Supabase
@st.cache_resource
def get_supabase():
    return init_supabase()

# Cache para leitura do CSV
@st.cache_data
def load_csv(file):
    return pd.read_csv(file, dtype=str)

def initialize_metrics():
    """Inicializa as métricas com valores zero"""
    return {
        "total": 0,
        "processados": 0,
        "atualizados": 0,
        "novos": 0,
        "erros": 0
    }

def process_batch(df_batch, supabase, stats):
    """Processa um lote de dados"""
    error_messages = []
    
    for index, row in df_batch.iterrows():
        try:
            print(f"\nProcessando linha {index + 1}")
            print(f"ID do Job: {row['ID']}")
            
            result = insert_or_update_job(row, supabase)
            
            if result in ['inserted', 'updated']:
                process_skills(row, supabase)
                stats["processados"] += 1
                if result == 'inserted':
                    stats["novos"] += 1
                else:
                    stats["atualizados"] += 1
            
        except Exception as e:
            stats["erros"] += 1
            error_message = f"Erro na linha {index + 1}, Job ID {row.get('ID', 'desconhecido')}: {str(e)}"
            print(error_message)
            error_messages.append(error_message)
    
    return stats, error_messages

def main():
    st.title("🚀 Importador de Jobs para Supabase")
    
    # Configurações na sidebar
    with st.sidebar:
        st.header("⚙️ Configurações")
        batch_size = st.slider("Tamanho do lote", 1, 100, 10)
        show_errors = st.checkbox("Mostrar Erros Detalhados", value=True)
    
    # Upload do arquivo
    uploaded_file = st.file_uploader("Escolha o arquivo CSV", type="csv")
    
    if uploaded_file:
        try:
            df = load_csv(uploaded_file)
            st.info(f"Arquivo carregado com {len(df)} registros")
            
            # Preview dos dados
            with st.expander("📊 Preview dos dados"):
                st.dataframe(df.head())
            
            if st.button("🚀 Iniciar Processamento"):
                supabase = get_supabase()
                stats = initialize_metrics()
                stats["total"] = len(df)
                
                # Containers para métricas e progresso
                col1, col2, col3, col4, col5 = st.columns(5)
                progress_bar = st.progress(0)
                error_container = st.empty()
                
                # Processamento em lotes
                all_errors = []
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size]
                    
                    # Processa o lote
                    stats, errors = process_batch(batch_df, supabase, stats)
                    all_errors.extend(errors)
                    
                    # Atualiza métricas
                    col1.metric("Total", stats["total"])
                    col2.metric("Processados", stats["processados"], 
                              f"{(stats['processados']/stats['total']*100):.1f}%")
                    col3.metric("Atualizados", stats["atualizados"])
                    col4.metric("Novos", stats["novos"])
                    col5.metric("Erros", stats["erros"])
                    
                    # Atualiza barra de progresso
                    progress = (i + len(batch_df)) / len(df)
                    progress_bar.progress(progress)
                    
                    time.sleep(0.1)
                
                # Mostra erros se houver
                if show_errors and all_errors:
                    with st.expander("❌ Erros encontrados"):
                        for error in all_errors:
                            st.error(error)
                
                # Resultado final
                st.success(f"""
                    ✅ Processamento concluído!
                    - Total processado: {stats['total']}
                    - Sucessos: {stats['processados']}
                    - Atualizados: {stats['atualizados']}
                    - Novos: {stats['novos']}
                    - Erros: {stats['erros']}
                """)
                
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {str(e)}")

if __name__ == "__main__":
    main() 