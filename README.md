<div align="center">

# Rastreador de aprovados | CPE

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Google Colab](https://img.shields.io/badge/Google_Colab-F9AB00?style=for-the-badge&logo=googlecolab&logoColor=white)

<p align="center">
  Automatização do cruzamento de dados entre alunos do Cursinho Popular EACH e listas oficiais de vestibulares (FUVEST, SISU, UNESP).
</p>

</div>



## O Problema
Atualmente, a conferência de aprovados é feita de forma manual, exigindo que a equipe do CPE analise listas com milhares de nomes para encontrar os alunos do cursinho. Isso é lento, cansativo e sujeito a erros humanos.

## A Solução
O rastreador de aprovados é um script em Python que utiliza comparação difusa de texto para identificar aprovações em segundos, mesmo que o nome do aluno tenha pequenas diferenças de grafia em relação à lista oficial.

### Principais funcionalidades
- **Rapidez:** Processa milhares de nomes em poucos segundos.
- **100% na nuvem:** Roda via Google Colab, sem necessidade de instalação local.
- **Relatório automático:** Gera uma planilha Excel pronta com os alunos aprovados e o grau de certeza.



## Tecnologias utilizadas

* **Python 3**: Linguagem base.
* **Pandas**: Manipulação e estruturação das tabelas de dados.
* **RapidFuzz**: Algoritmos de correspondência de textos de alta performance.
* **Unidecode**: Normalização de texto (remoção de acentos e de caracteres especiais).
* **Google Colab**: Ambiente de execução acessível.



## Como executar

Para executar o script siga os passos:

1.  **Acesse o notebook:** Clique no arquivo `rastreador_aprovados.ipynb` acima ou abra no Google Colab.
2.  **Prepare os arquivos:** Tenha em seu computador:
    * Planilha de alunos do CPE (`.xlsx` ou `.csv`).
    * Lista oficial de aprovados (`.xlsx` ou `.csv`).
3.  **Rode o programa:**
    * No Colab, clique no ícone de "play" na célula principal.
    * O sistema instalará as dependências automaticamente.
4.  **Faça o upload:**
    * Quando solicitado, envie a planilha de alunos.
    * Em seguida, envie a lista oficial.
5.  **Baixe o resultado:**
    * O script gerará o arquivo `resultado_cruzamento.xlsx`.
    * O download iniciará automaticamente (ou estará disponível na aba de arquivos).



## Exemplo de resultado

O sistema classifica os resultados para facilitar a conferência humana:

| Aluno CPE | Nome na lista oficial | Similaridade | Status |
| :--- | :--- | :---: | :--- |
| **Carlos Souza** | CARLOS SOUZA | 100% | ✅ Aprovado |
| **Ana V. Silva** | ANA VITORIA SILVA | 88% | ⚠️ Verificar |
| **João Pedro** | PEDRO ALMEIDA | 40% | ❌ Ignorado |



## Próximos passos

A evolução deste projeto inclui desenvolver uma interface gráfica, realizando a migração do script para **Streamlit**, criando uma interação amigável onde o usuário apenas arrasta os arquivos, sem ver o código.



<div align="center">

**Desenvolvido com 🧡💙 por Inovatec**

</div>
