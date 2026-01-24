import streamlit as st
from streamlit_extras.stylable_container import stylable_container
import rastreador_aprovados as backend

# Inicialização de sessão
if "df_resultado_conferencia" not in st.session_state:
    st.session_state.df_resultado_conferencia = None

# Funções de Callback
def realiza_conferencia():
    if arquivo_lista_alunos and arquivo_lista_vestibular:
        with st.spinner('Lendo arquivos e cruzando dados... Isso pode levar alguns segundos.'):
            st.session_state.df_resultado_conferencia = backend.processar_conferencia(
                arquivo_lista_alunos,
                arquivo_lista_vestibular,
                st.session_state.opcao == "Nome + CPF"
            )
    else:
        st.error("Por favor, faça o upload dos dois arquivos.")

# Configuração da Página
st.set_page_config(
    page_title="Rastreador de Aprovados",
    page_icon="🦉",
    layout="centered"
)

# Estilização CSS (Ajustada para suportar Modo Claro e Escuro)
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700;800&display=swap');

/* Fonte global, mas SEM forçar a cor branca (o tema define isso agora) */
html, body, [data-testid="stAppViewContainer"], .stApp {
  font-family: 'Poppins', sans-serif !important;
}

/* Título: Removemos a cor fixa para ele aparecer preto no modo claro */
.titulo {
  font-weight: 800; font-size: 28px !important; margin: 0;
}

/* Botão continua laranja com texto branco */
div.stButton > button {
  background-color: #ef7b17 !important; color: white !important; border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# Cabeçalho
col_img, col_titulo = st.columns([1, 9], vertical_alignment="center")
with col_img:
   st.image("logo.png", width=120)
with col_titulo:   
 st.markdown("<h2 class='titulo'> Rastreador de<br>Aprovados Universal</h2>", unsafe_allow_html=True)

# --- ÁREA PRINCIPAL ---
# Aqui adicionamos 'color: white' para garantir que DENTRO do fundo azul o texto seja sempre branco
with stylable_container(
    key="meu_card",
    css_styles="{background-color: #15355B; border-radius: 15px; padding: 30px; color: white;}"
):
    st.markdown('<h3 style="color:white;">Conferência de Listas</h3>', unsafe_allow_html=True)
    st.info("Agora aceita arquivos PDF e TXT diretamente! O sistema buscará o nome dos alunos dentro do arquivo da lista oficial.")

    # Opções de Método
    st.radio(
        "Método de Validação:",
        ["Nome completo", "Nome + CPF"],
        horizontal=True,
        key="opcao",
        help="Se escolher Nome + CPF, o sistema procurará o nome do aluno e verificará se algum fragmento do CPF dele está próximo no texto."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('**1. Alunos do Cursinho**')
        arquivo_lista_alunos = st.file_uploader(
            "Upload Alunos", 
            type=["csv", "xlsx"], 
            key="a1", 
            label_visibility="collapsed"
        )

    with col2:
        st.markdown('**2. Lista Oficial**')
        arquivo_lista_vestibular = st.file_uploader(
            "Upload Lista Oficial", 
            type=["csv", "xlsx", "pdf", "txt"], 
            key="a2", 
            label_visibility="collapsed"
        )

    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("🔍 Rastrear Aprovados", on_click=realiza_conferencia, use_container_width=True):
        pass

    # Exibição dos Resultados
    if st.session_state.df_resultado_conferencia is not None:
        st.write("### Resultado da Análise:")
        st.dataframe(st.session_state.df_resultado_conferencia, use_container_width=True)