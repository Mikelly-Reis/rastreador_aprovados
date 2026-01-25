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

# Configuração da página
st.set_page_config(
    page_title="Rastreador de Aprovados",
    page_icon="🦉",
    layout="centered"
)

# Estilização CSS
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700;800&display=swap');

/* Fonte global */
html, body, [data-testid="stAppViewContainer"], .stApp {
  font-family: 'Poppins', sans-serif !important;
}

/* Título */
.titulo { font-weight: 800; font-size: 28px !important; margin: 0; }

/* Botão principal */
div.stButton > button {
  background-color: #ef7b17 !important;
  color: white !important;
  border-radius: 8px !important;
}

/* Mensagens do Dropzone */
[data-testid="stFileUploaderDropzoneInstructions"] > div > span { display: none; }
[data-testid="stFileUploaderDropzoneInstructions"] > div::after {
   content: "Solte seu arquivo aqui";
   display: block;
   font-size: 16px;
   margin-bottom: 5px;
}
[data-testid="stFileUploaderDropzoneInstructions"] > div > small { display: none; }
[data-testid="stFileUploaderDropzoneInstructions"] > div::before {
   content: "Limite de 1GB • CSV, XLSX, PDF, TXT";
   display: block;
   font-size: 12px;
   color: #666;
   margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)

# Cabeçalho
col_img, col_titulo = st.columns([1, 9], vertical_alignment="center")
with col_img:
   st.image("logo.png", width=120)
with col_titulo:   
 st.markdown("<h2 class='titulo'> Rastreador de<br>Aprovados</h2>", unsafe_allow_html=True)

# Área principal do app
# Aqui adicionamos 'color: white' para garantir que DENTRO do fundo azul o texto seja sempre branco
with stylable_container(
    key="meu_card",
    css_styles="{background-color: #15355B; border-radius: 15px; padding: 30px; color: white;}"
):
    st.markdown('<h3 style="color:white;">Conferência de listas</h3>', unsafe_allow_html=True)
    st.info("Agora aceita arquivos PDF e TXT diretamente! O sistema buscará o nome dos alunos dentro do arquivo da lista oficial.")

    # Opções de método
    st.radio(
        "Método de validação:",
        ["Nome completo", "Nome + CPF"],
        horizontal=True,
        key="opcao",
        help="Se escolher nome + CPF, o sistema procurará o nome do aluno e verificará se algum fragmento do CPF dele está próximo no texto."
    )

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('**1. Alunos do cursinho**')
        arquivo_lista_alunos = st.file_uploader(
            "Upload da lista de alunos", 
            type=["csv", "xlsx"], 
            key="a1", 
            label_visibility="collapsed"
        )

    with col2:
        st.markdown('**2. Lista oficial**')
        arquivo_lista_vestibular = st.file_uploader(
            "Upload da lista oficial", 
            type=["csv", "xlsx", "pdf", "txt"], 
            key="a2", 
            label_visibility="collapsed"
        )

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("Buscar", on_click=realiza_conferencia, width='stretch'):
        pass

    # Exibição dos resultados
    if st.session_state.df_resultado_conferencia is not None:
        st.write("### Resultado da análise:")
        st.dataframe(st.session_state.df_resultado_conferencia, width='stretch')
