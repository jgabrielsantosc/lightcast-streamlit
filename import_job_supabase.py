import streamlit as st
import pandas as pd
from utils.data_processor import prepare_job_data, process_skills
from utils.supabase_handler import init_supabase, verify_company, verify_title
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

def create_status_containers():
    """Cria containers para status e métricas"""
    status_area = st.container()
    col1, col2, col3, col4, col5 = status_area.columns(5)
    
    metrics = {
        "total": col1.empty(),
        "processados": col2.empty(),
        "atualizados": col3.empty(),
        "novos": col4.empty(),
        "erros": col5.empty()
    }
    
    progress = st.progress(0)
    log = st.empty()
    error_log = st.empty()
    
    return metrics, progress, log, error_log

def process_batch(batch_df, supabase, metrics, total_rows, current_stats):
    """Processa um lote de registros"""
    for idx, row in batch_df.iterrows():
        try:
            # Verifica/cria dependências
            verify_title(row, supabase)
            verify_company(row, supabase)
            
            # Prepara e insere/atualiza job
            job_data = prepare_job_data(row)
            existing_job = supabase.table('jobs').select('*').eq('id', row['ID']).execute()
            
            if existing_job.data:
                supabase.table('jobs').update(job_data).eq('id', row['ID']).execute()
                current_stats["atualizados"] += 1
            else:
                supabase.table('jobs').insert(job_data).execute()
                current_stats["novos"] += 1
            
            process_skills(row, supabase)
            current_stats["processados"] += 1
            
        except Exception as e:
            current_stats["erros"] += 1
            current_stats["error_details"].append(f"Erro no registro {idx + 1}: {str(e)}")
            if len(current_stats["error_details"]) > 5:
                current_stats["error_details"].pop(0)
        
        # Atualiza métricas
        progress = (current_stats["processados"] + current_stats["erros"]) / total_rows
        update_metrics(metrics, current_stats, progress)
        
    return current_stats

def update_metrics(metrics, stats, progress):
    """Atualiza as métricas na interface"""
    metrics["total"].metric("Total", stats["total"], help="Total de registros a processar")
    metrics["processados"].metric("Processados", stats["processados"], 
                                delta=f"{(stats['processados']/stats['total']*100):.1f}%")
    metrics["atualizados"].metric("Atualizados", stats["atualizados"])
    metrics["novos"].metric("Novos", stats["novos"])
    metrics["erros"].metric("Erros", stats["erros"], 
                          delta_color="inverse" if stats["erros"] > 0 else "off")

def main():
    # Estilo personalizado
    st.markdown("""
        <style>
        .main { padding: 2rem; }
        .stProgress > div > div > div > div { background-color: #1f77b4; }
        .status-box {
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 1rem 0;
        }
        .success { background-color: #d4edda; color: #155724; }
        .error { background-color: #f8d7da; color: #721c24; }
        </style>
    """, unsafe_allow_html=True)

    # Interface principal
    st.title("🚀 Importador de Jobs para Supabase")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configurações")
        batch_size = st.slider("Tamanho do lote", 1, 100, 30)
        retry_attempts = st.number_input("Tentativas em caso de erro", 1, 5, 3)
        retry_delay = st.number_input("Delay entre tentativas (seg)", 1, 10, 2)
        
        st.header("🔍 Opções")
        debug_mode = st.checkbox("Modo Debug", value=False)
        show_errors = st.checkbox("Mostrar Erros Detalhados", value=True)
    
    # Upload do arquivo
    uploaded_file = st.file_uploader("Escolha o arquivo CSV", type="csv")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            total_rows = len(df)
            
            # Preview dos dados
            with st.expander("📊 Preview dos dados", expanded=False):
                st.dataframe(df.head())
            
            # Criar containers de status
            metrics, progress_bar, log_area, error_area = create_status_containers()
            
            if st.button("🚀 Iniciar Processamento", type="primary"):
                supabase = get_supabase()
                
                # Estatísticas iniciais
                stats = {
                    "total": total_rows,
                    "processados": 0,
                    "atualizados": 0,
                    "novos": 0,
                    "erros": 0,
                    "error_details": []
                }
                
                # Processamento em lotes
                for batch_start in range(0, total_rows, batch_size):
                    batch_end = min(batch_start + batch_size, total_rows)
                    batch_df = df.iloc[batch_start:batch_end]
                    
                    log_area.info(f"Processando lote {batch_start//batch_size + 1} de {(total_rows + batch_size - 1)//batch_size}")
                    
                    # Processa o lote
                    stats = process_batch(batch_df, supabase, metrics, total_rows, stats)
                    
                    # Mostra erros se houver
                    if show_errors and stats["error_details"]:
                        error_area.error("\n".join(stats["error_details"]))
                    
                    # Atualiza barra de progresso
                    progress = (batch_end) / total_rows
                    progress_bar.progress(progress)
                    
                    time.sleep(0.1)  # Pequena pausa entre lotes
                
                # Resultado final
                st.success(f"""
                    ✅ Processamento concluído!
                    - Total: {stats['total']}
                    - Processados com sucesso: {stats['processados']} ({stats['processados']/stats['total']*100:.1f}%)
                    - Atualizados: {stats['atualizados']}
                    - Novos: {stats['novos']}
                    - Erros: {stats['erros']} ({stats['erros']/stats['total']*100:.1f}%)
                """)
                
                # Download do log de erros se houver
                if stats["error_details"]:
                    error_log = "\n".join(stats["error_details"])
                    st.download_button(
                        "📥 Download log de erros",
                        error_log,
                        "error_log.txt",
                        "text/plain"
                    )
        
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {str(e)}")

if __name__ == "__main__":
    main() 