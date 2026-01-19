import streamlit as st

from streamlit_extras.stylable_container import stylable_container



st.set_page_config(
    page_title="Rastreador",
    page_icon="🦉",
    layout="centered"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@600;700;800&display=swap');

            
/* Aplica ao corpo do app */
    html, body, [data-testid="stAppViewContainer"], .stApp {
        font-family: 'Poppins', sans-serif !important;
    }


  

.titulo {
    font-family: 'Poppins', sans-serif !important;
    font-weight: 800;
    letter-spacing: -0.02em;
    line-height: 1.1;
    font-size: 20px !important;
}
            
            /* Estiliza o rótulo (Label) "Método:" */
    div[data-testid="stRadio"] > label {
        font-size: 20px !important;
        color: #ffff !important;
        
    }
    div[data-testid="stRadio"] {
    margin-left: 20px !important;
}

    /* Estiliza as opções ("Nome Completo", etc) */
    div[data-testid="stRadio"] div[role="radiogroup"] label p {
        font-size: 15px !important;
        color: #ffff !important;
        
    }
            
            /* 2. ESTILO DOS UPLOADERS (Laranja Translúcido) */
    [data-testid='stFileUploaderDropzone'] {
        min-height: 100px !important;
        margin: 0 auto !important;
        width: 90% !important;
        
        /* Laranja translúcido */
        background-color: rgba(239, 123, 23, 0.15) !important; 
        border: 2px dashed rgba(239, 123, 23, 0.5) !important;
        border-radius: 8px !important;
        
        position: relative;
        transition: all 0.3s ease-in-out;
    }

    /* Hover: Fica mais forte quando passa o mouse */
    [data-testid='stFileUploaderDropzone']:hover {
        background-color: rgba(239, 123, 23, 0.25) !important;
        border-color: #ef7b17 !important;
    }

    /* 3. LIMPEZA (Esconde os textos e ícones originais do Streamlit) */
    [data-testid='stFileUploaderDropzone'] > div:first-child,
    [data-testid='stFileUploaderDropzone'] span, 
    [data-testid='stFileUploaderDropzone'] small,
    [data-testid='stFileUploaderDropzone'] svg {
        display: none !important;
    }

    /* 4. TEXTO NOVO (Configuração Base Centralizada) */
    [data-testid='stFileUploaderDropzone']::after {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        text-align: center;
        font-weight: bold;
        font-size: 14px;
        color: white; 
        width: 80%;
        pointer-events: none;
        white-space: pre-wrap; /* Permite pular linha */
        content: "Solte o arquivo aqui"; /* Texto de segurança */
    }
 
             /* Estiliza o botão BUSCAR */
    div.stButton > button {
        background-color: #ef7b17 !important; /* Sua cor laranja */
        color: white !important;              /* Texto branco */
        border: 1px solid #ef7b17 !important; /* Borda da mesma cor */
        font-weight: bold !important;
        border-radius: 8px !important;        /* Borda arredondada */
        width: auto !important;
        float: right !important;
                     
                                            

    /* Efeito ao passar o mouse (fica um pouco mais escuro/interativo) */
    div.stButton > button:hover {
        background-color: #d6690f !important; /* Um laranja levemente mais escuro */
        border-color: #d6690f !important;
        color: white !important;
    }
    
    /* Efeito ao clicar (foco) - Remove borda vermelha padrão do Streamlit */
    div.stButton > button:focus {
        box-shadow: none !important;
        color: white !important;
    }
}
</style>
""", unsafe_allow_html=True)

#-------------------------------------------------------------------------------


col_img, col_titulo = st.columns([1, 9], vertical_alignment="center")

with col_img:
   st.image("15.png", width=120)
with col_titulo:   
 st.markdown(
    "<h2 class='titulo'> Rastreador de<br>Aprovados </h2>", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["Conferência", "Conversor PDF"])

with tab1:

    with stylable_container(
        key="meu_card",
        css_styles="""
            {
                background-color: #15355B; 
                border-radius: 15px;
                padding: 50px;
                box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
            }
        """
    ):
        
        
        st.markdown('<h2 style="color:white; padding:0; font-size: 25px; text-align: left;   margin-left: 20px !important;">Conferência de Listas</h2>', unsafe_allow_html=True)

# ---------------
        st.markdown('<div style="margin-top: -30px;"></div>', unsafe_allow_html=True) 
# -----------------------

        st.radio(
    "Método:",
    ["Nome Completo", "Nome + CPF"],
    horizontal=True
)

        col1, col2 = st.columns(2)

        with col1:
    
         st.markdown('<h2 style="color:white; padding:5px; font-size: 15px; text-align: center;  ">🧑‍🏫Lista de Alunos</h2>', unsafe_allow_html=True)
         st.file_uploader("Arquivo 1", type=["csv", "xlsx"], key="a1", label_visibility="collapsed")

        with col2:
         st.markdown('<h2 style="color:white; padding:5px; font-size: 15px; text-align: center;  ">📑Lista do Vestibular</h2>', unsafe_allow_html=True)
         st.file_uploader("Arquivo 2", type=["csv", "xlsx"], key="a2", label_visibility="collapsed")
    

         col_vazia, col_botao = st.columns([2, 1])
         with col_botao:
          st.button("Buscar")


with tab2:
     with stylable_container(
        key="meu_card2",
        css_styles="""
            {
                background-color: #15355B;
                border-radius: 15px;
                padding: 50px;
                box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
            }
        """
    ):
        
        st.markdown('<h2 style="color:white; padding:0; font-size: 25px; text-align: left; ">Extrator de Tabelas em Arquivos PDF<br></h2>', unsafe_allow_html=True)
        st.markdown('<h7 style="color:white; padding:0; font-size: 15px; text-align: left; margin-top: 10px; ">Use isso para converter listas de aprovados que estão no formato PDF.<br> O sistema tentará criar um arquivo organizado no formato padrão do Excel.</h7>', unsafe_allow_html=True)

        st.markdown('<h2 style="color:white; padding:5px; font-size: 15px; text-align: center;  ">🗂️</h2>', unsafe_allow_html=True)
        st.file_uploader("PDF", type=["pdf"],  label_visibility="collapsed", key="conv_pdf")

        col_vazia2, col_botao2 = st.columns([2, 1])
        with col_botao2:
         st.button("Converter para Excel", key= "btn_tb2")