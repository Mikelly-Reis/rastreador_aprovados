import requests

url = "https://rastreador-aprovados.streamlit.app/"

try:
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Sucesso! O app foi visitado. Status: {response.status_code}")
    else:
        print(f"O app retornou um erro: {response.status_code}")
except Exception as e:
    print(f"Erro ao tentar acessar o app: {e}")