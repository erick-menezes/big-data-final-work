# Big Data - Projeto de extensão

## Sobre o projeto

Projeto criado para o projeto de extensão da matéria de Big Data aplicada na faculdade Estácio de Sá.

Neste projeto, criamos um conversor de dados que pega vários arquivos em PDF dentro de uma pasta específica (padrão: input), lê o documento de acordo com o padrão dele e retorna os dados coletados em um Excel em uma outra pasta (padrão: output).

Com este resultado final, nós podemos utilizar o Power BI para gerar informações que sejam úteis para a parte interessada, dando vários insights sobre o aglomerado de dados informado inicialmente.

## Como rodar o projeto

Para rodar o projeto em sua máquina, você precisa ter os seguintes requisitos:

- Java (versão recomendada: >= 17)
- Python (versão recomendada: >= 3.9.4)

Crie um **venv** para instalar as dependências necessárias do projeto.
```powershell
python -m venv venv
```

Instale as dependências.
```powershell
pip install -r requirements.txt
```

Adicione as pastas **output, input e tmp** na raiz do projeto. Assim o projeto conseguirá transferir os arquivos corretamente. A estrutura principal ficará assim:

```powershell
├── input
├── output
├── src
├── tmp
├── venv
├── .gitignore
├── README.md
└── requirements.txt
```

**Obs: Insira os arquivos PDF que deseja extrair os dados dentro da pasta input.**

Por fim, execute o projeto.
```powershell
py src/main.py
```
