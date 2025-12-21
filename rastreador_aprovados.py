import pandas as pd
import re
import io
from rapidfuzz import process, fuzz
from unidecode import unidecode
from pypdf import PdfReader

# Funções auxiliares:
def normalizar_texto(texto):
    if pd.isna(texto) or texto == "":
        return ""
    # Converte para string, remove acentos e joga para caixa alta
    texto_limpo = unidecode(str(texto).upper())
    return texto_limpo.strip()

def limpar_numeros(valor):
    if pd.isna(valor):
        return ""
    # Mantém apenas números e letras, nenhum outro caractere, para CPF/outros docs
    return re.sub(r'[^0-9X]', '', str(valor).upper())


# Módulo de conversão de PDF:
def extrair_tabela_pdf(arquivo_pdf):
    reader = PdfReader(arquivo_pdf)
    dados_extraidos = []
    
    # Regex para capturar pares: (código/CPF) + (nome)
    # Procura sequências de números/pontos seguidos de letras
    regex_padrao_colunas = re.compile(r'([\d\.\-\*]{3,})\s+([A-ZÀ-Ú\s\.]+)(?=\s[\d\.\-\*]{3,}|\s*$)', re.IGNORECASE)
    
    for page in reader.pages:
        texto = page.extract_text()
        if not texto: continue
            
        linhas = texto.split('\n')
        for linha in linhas:
            # Tenta encontrar o padrão "CPF Nome" na linha (pode ter várias colunas na mesma linha)
            matches = regex_padrao_colunas.findall(linha)
            
            if matches:
                # Se achou padrão (CPF, nome), adiciona a tupla
                for cpf_bruto, nome_bruto in matches:
                    if len(nome_bruto.strip()) > 3: # Ignora lixo
                        dados_extraidos.append({
                            "Código/CPF": cpf_bruto.strip(),
                            "Nome": nome_bruto.strip()
                        })
            else:
                # Se não achou números, assume que é uma lista só de nomes (ex: UNESP)
                # Remove números soltos de página ou cabeçalho
                linha_limpa = re.sub(r'[0-9\.\-]{3,}', '', linha).strip()
                # Quebra por múltiplos espaços (caso tenha colunas de nomes sem CPF)
                nomes = re.split(r'\s{2,}', linha_limpa)
                for nome in nomes:
                    if len(nome) > 4 and "Página" not in nome:
                        dados_extraidos.append({
                            "Nome": nome.strip()
                        })

    # Cria o DataFrame final
    df = pd.DataFrame(dados_extraidos)
    
    # Remove duplicatas e linhas vazias
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
        
    return df


# Módulo de rastreamento:
def identificar_colunas(df):
    # Tenta adivinhar quais colunas correspondem a NOME e CPF/DOC.
    cols_lower = [c.lower() for c in df.columns]
    
    # Palavras-chave para "nome"
    keywords_nome = ['nome', 'candidato', 'aluno', 'name']
    col_nome = next((df.columns[i] for i, c in enumerate(cols_lower) if any(k in c for k in keywords_nome)), None)
    
    # Se não achar, pega a primeira coluna de texto
    if not col_nome:
        cols_texto = df.select_dtypes(include=['object']).columns
        col_nome = cols_texto[0] if len(cols_texto) > 0 else df.columns[0]

    # Palavras-chave para "documento"
    keywords_doc = ['cpf', 'doc', 'inscrição', 'inscricao', 'código', 'codigo']
    col_cpf = next((df.columns[i] for i, c in enumerate(cols_lower) if any(k in c for k in keywords_doc)), None)

    return col_nome, col_cpf


# Função para carregar DataFrame de CSV ou Excel:
def carregar_dataframe(arquivo):
    nome = arquivo.name.lower()
    if nome.endswith('.csv'):
        try:
            return pd.read_csv(arquivo, dtype=str)
        except:
            arquivo.seek(0)
            return pd.read_csv(arquivo, sep=';', dtype=str)
    else:
        return pd.read_excel(arquivo, dtype=str)


# Função principal chamada pelo botão de rastreamento:
def processar_conferencia(arquivo_alunos, arquivo_lista_oficial, usar_cpf=False):
    # Carregamento
    df_alunos = carregar_dataframe(arquivo_alunos)
    df_oficial = carregar_dataframe(arquivo_lista_oficial)
    
    # Identificação de colunas
    col_nome_aluno, col_cpf_aluno = identificar_colunas(df_alunos)
    col_nome_lista, col_cpf_lista = identificar_colunas(df_oficial)
    
    # Validação de segurança
    if usar_cpf and (not col_cpf_aluno or not col_cpf_lista):
        return pd.DataFrame({"Erro": ["Você escolheu conferência por CPF, mas não encontrei coluna de CPF/Documento em um dos arquivos."]})

    # Preparação para busca (normalização)
    # Lista de nomes normalizados da lista oficial para busca rápida
    lista_nomes_oficial_norm = [normalizar_texto(x) for x in df_oficial[col_nome_lista].dropna()]
    # A lista original é mantida para exibir no resultado
    lista_nomes_oficial_orig = df_oficial[col_nome_lista].dropna().tolist()

    resultados = []
    
    # Loop de cruzamento
    for idx, row in df_alunos.iterrows():
        nome_aluno_real = str(row[col_nome_aluno])
        nome_aluno_busca = normalizar_texto(nome_aluno_real)
        
        if len(nome_aluno_busca) < 4: continue

        # Busca por nome (aproximada)
        match = process.extractOne(
            nome_aluno_busca, 
            lista_nomes_oficial_norm, 
            scorer=fuzz.token_sort_ratio, 
            score_cutoff=85 # Aceita pequenas variações (Souza vs Sousa)
        )

        if match:
            nome_encontrado_norm, score, index_match = match
            nome_encontrado_real = lista_nomes_oficial_orig[index_match]
            
            # Decisão de status
            status = "Em análise"
            confirma_match = False
            
            if usar_cpf:
                # Validação por CPF (modo rigoroso) ---
                doc_aluno = limpar_numeros(row[col_cpf_aluno])
                doc_lista = limpar_numeros(df_oficial.iloc[index_match][col_cpf_lista])
                
                # Verifica se um contém o outro (ex: 123456 na lista vs 12345678 no cadastro)
                if doc_aluno and doc_lista and (doc_aluno in doc_lista or doc_lista in doc_aluno):
                    status = "✅ APROVADO (nome e CPF confirmados)"
                    confirma_match = True
                else:
                    status = "⚠️ NOME IGUAL, DOCUMENTO DIFERENTE (verificar homônimo)"
                    # Se o score de nome for perfeito mas CPF não bate, mostramos como alerta
                    if score >= 98: confirma_match = True 
            
            else:
                # Apenas nome (modo simples) ---
                if score >= 97:
                    status = "✅ APROVADO (nome idêntico)"
                    confirma_match = True
                elif score >= 90:
                    status = "🔍 PROVÁVEL (verificar sobrenome)"
                    confirma_match = True
            
            # Adiciona ao relatório se passou no filtro
            if confirma_match:
                dados_resultado = {
                    "Aluno CPE": nome_aluno_real,
                    "Nome na lista": nome_encontrado_real,
                    "Similaridade": f"{score:.1f}%",
                    "Status": status
                }
                # Se tiver CPF, adiciona para conferência visual
                if col_cpf_aluno: dados_resultado["Doc Aluno"] = row[col_cpf_aluno]
                if usar_cpf and col_cpf_lista: dados_resultado["Doc Lista"] = df_oficial.iloc[index_match][col_cpf_lista]
                
                resultados.append(dados_resultado)

    # Finalização
    if not resultados:
        return pd.DataFrame({"Resultado": ["Nenhum match encontrado com os critérios atuais."]})
        
    return pd.DataFrame(resultados).sort_values(by="Status")