import pandas as pd
import re
import io
import pdfplumber
import gc
import streamlit as st  # Necessário para o cache
from rapidfuzz import process, fuzz
from unidecode import unidecode
from pypdf import PdfReader 

# ==========================================
# FUNÇÕES DE LIMPEZA E UTILITÁRIOS
# ==========================================

def normalizar_texto(texto):
    """Remove acentos, caixa alta e espaços extras."""
    if pd.isna(texto) or texto == "":
        return ""
    texto_limpo = unidecode(str(texto).upper())
    texto_limpo = texto_limpo.replace('\n', ' ')
    return re.sub(r'\s+', ' ', texto_limpo).strip()

def limpar_numeros(valor):
    """Deixa apenas números (para CPF/RG)."""
    if pd.isna(valor):
        return ""
    return re.sub(r'\D', '', str(valor))

def obter_fragmentos_cpf(cpf):
    """Gera fragmentos do CPF para busca flexível."""
    cpf_limpo = limpar_numeros(cpf)
    if len(cpf_limpo) < 11:
        return []
    
    return [
        cpf_limpo[0:3],   # 123.***
        cpf_limpo[3:6],   # ***.456.***
        cpf_limpo[6:9],   # ***.***.789
        cpf_limpo[9:11]   # ***-00
    ]

# O cache evita que o Streamlit re-leia o arquivo toda vez que você interage com a tela
@st.cache_data(ttl=3600, show_spinner=False) 
def carregar_dataframe(arquivo):
    """Lê CSV ou Excel com cache."""
    nome = arquivo.name.lower()
    if nome.endswith('.csv'):
        try:
            return pd.read_csv(arquivo, dtype=str)
        except:
            arquivo.seek(0)
            return pd.read_csv(arquivo, sep=';', dtype=str)
    else:
        return pd.read_excel(arquivo, dtype=str)

def identificar_colunas(df):
    """Tenta adivinhar colunas de Nome e CPF."""
    cols_lower = [c.lower() for c in df.columns]
    
    keywords_nome = ['nome', 'candidato', 'aluno', 'estudante']
    col_nome = next((df.columns[i] for i, c in enumerate(cols_lower) if any(k in c for k in keywords_nome)), None)
    if not col_nome:
        cols_texto = df.select_dtypes(include=['object']).columns
        col_nome = cols_texto[0] if len(cols_texto) > 0 else df.columns[0]

    keywords_doc = ['cpf', 'doc', 'inscrição', 'inscricao', 'rg']
    col_cpf = next((df.columns[i] for i, c in enumerate(cols_lower) if any(k in c for k in keywords_doc)), None)

    return col_nome, col_cpf

# ==========================================
# LÓGICA DE LEITURA (PDF / TXT) OTIMIZADA
# ==========================================

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_texto_pdf(arquivo_pdf):
    """Extrai texto do PDF e limpa memória imediatamente."""
    texto_completo = ""
    try:
        with pdfplumber.open(arquivo_pdf) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t: texto_completo += " " + t
        
        # Limpeza de memória explícita
        gc.collect() 
        return normalizar_texto(texto_completo)
    except:
        return ""

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_texto_txt(arquivo_txt):
    """Lê arquivo TXT."""
    try:
        conteudo = arquivo_txt.read().decode("utf-8")
    except UnicodeDecodeError:
        arquivo_txt.seek(0)
        conteudo = arquivo_txt.read().decode("latin-1")
    return normalizar_texto(conteudo)

def buscar_em_texto_corrido(df_alunos, texto_norm, col_nome, col_cpf, usar_validacao_cpf):
    """Busca otimizada no textão extraído."""
    resultados = []
    total_chars = len(texto_norm)

    for idx, row in df_alunos.iterrows():
        nome_original = row[col_nome]
        nome_busca = normalizar_texto(nome_original)
        
        if len(nome_busca) < 4: continue 

        index_encontrado = texto_norm.find(nome_busca)
        
        if index_encontrado != -1:
            match_cpf = False
            frag_encontrado = ""
            status = ""
            obs = ""
            
            # Validação de CPF (Opcional)
            if usar_validacao_cpf and col_cpf:
                cpf_aluno = row[col_cpf]
                fragmentos = obter_fragmentos_cpf(cpf_aluno)
                
                if fragmentos:
                    inicio = max(0, index_encontrado - 50)
                    fim = min(total_chars, index_encontrado + len(nome_busca) + 50)
                    contexto_numerico = re.sub(r'\D', '', texto_norm[inicio:fim])
                    
                    for frag in fragmentos:
                        if frag in contexto_numerico:
                            match_cpf = True
                            frag_encontrado = frag
                            break
                    
                    if match_cpf:
                        status = "✅ Aprovado (Confirmado)"
                        obs = f"Nome e fragmento CPF ({frag_encontrado}) encontrados."
                    else:
                        status = "⚠️ Verificar (CPF Divergente)"
                        obs = "Nome encontrado, mas CPF não bateu no contexto."
                else:
                    status = "✅ Aprovado (Nome encontrado)"
                    obs = "Sem CPF para validar."
            else:
                status = "✅ Aprovado (Nome encontrado)"
                obs = "Validação apenas por nome."

            resultados.append({
                "Aluno CPE": nome_original,
                "Nome Detectado": nome_busca,
                "Similaridade": "100%",
                "Status": status,
                "Observação": obs
            })

    # Limpeza final pós-processamento
    gc.collect()
    
    if not resultados:
        return pd.DataFrame({"Resultado": ["Nenhum aluno encontrado."]})
        
    return pd.DataFrame(resultados).sort_values(by="Status")

# ==========================================
# CONTROLADOR PRINCIPAL
# ==========================================

def processar_conferencia(arquivo_alunos, arquivo_lista, usar_cpf=False):
    # 1. Carregar Alunos
    df_alunos = carregar_dataframe(arquivo_alunos)
    col_nome, col_cpf = identificar_colunas(df_alunos)
    
    if not col_nome:
        return pd.DataFrame({"Erro": ["Coluna de nomes não identificada na planilha de alunos."]})

    nome_lista = arquivo_lista.name.lower()
    texto_extraido = None
    
    # ROTA A: Arquivos de Texto (PDF/TXT)
    if nome_lista.endswith('.pdf'):
        texto_extraido = carregar_texto_pdf(arquivo_lista)
    elif nome_lista.endswith('.txt'):
        texto_extraido = carregar_texto_txt(arquivo_lista)

    if texto_extraido is not None:
        if not texto_extraido:
            return pd.DataFrame({"Erro": ["Arquivo vazio ou ilegível."]})
        return buscar_em_texto_corrido(df_alunos, texto_extraido, col_nome, col_cpf, usar_cpf)

    # ROTA B: Excel/CSV
    else:
        df_oficial = carregar_dataframe(arquivo_lista)
        col_nome_off, col_cpf_off = identificar_colunas(df_oficial)
        
        lista_nomes_norm = [normalizar_texto(x) for x in df_oficial[col_nome_off].dropna()]
        lista_nomes_orig = df_oficial[col_nome_off].dropna().tolist()
        
        resultados = []
        
        for idx, row in df_alunos.iterrows():
            nome_real = str(row[col_nome])
            nome_busca = normalizar_texto(nome_real)
            if len(nome_busca) < 4: continue

            match = process.extractOne(nome_busca, lista_nomes_norm, scorer=fuzz.token_sort_ratio, score_cutoff=85)

            if match:
                nome_enc_norm, score, idx_match = match
                nome_enc_real = lista_nomes_orig[idx_match]
                
                status = "Em análise"
                adicionar = False
                obs = ""
                
                if usar_cpf and col_cpf and col_cpf_off:
                    doc_aluno = limpar_numeros(row[col_cpf])
                    doc_lista = limpar_numeros(df_oficial.iloc[idx_match][col_cpf_off])
                    fragmentos = obter_fragmentos_cpf(doc_aluno)
                    
                    match_doc = any(f in doc_lista for f in fragmentos) if fragmentos else False
                    
                    if match_doc:
                        status = "✅ Aprovado"
                        obs = "Nome e Documento conferem."
                        adicionar = True
                    else:
                        status = "⚠️ Verificar Homônimo"
                        obs = "Documento diverge."
                        if score >= 98: adicionar = True
                else:
                    if score >= 95: status, adicionar = "✅ Aprovado", True
                    elif score >= 88: status, adicionar = "🔍 Verificar grafia", True
                
                if adicionar:
                    resultados.append({
                        "Aluno CPE": nome_real,
                        "Nome na Lista": nome_enc_real,
                        "Similaridade": f"{score:.1f}%",
                        "Status": status,
                        "Observação": obs
                    })
        
        gc.collect() # Limpeza final
        if not resultados:
            return pd.DataFrame({"Resultado": ["Nenhum match encontrado."]})
        return pd.DataFrame(resultados).sort_values(by="Status")

def extrair_tabela_pdf(arquivo_pdf):
    # Versão simplificada para não pesar
    try:
        reader = PdfReader(arquivo_pdf)
        dados = []
        # Limita a 5 páginas para pré-visualização para não travar
        for i, page in enumerate(reader.pages):
            if i > 5: break 
            if page.extract_text():
                dados.append({"Página": i+1, "Conteúdo Parcial": page.extract_text()[:300] + "..."})
        return pd.DataFrame(dados)
    except:
        return pd.DataFrame()