#########################################################
# Alessandro Miranda Gonçalves                          #
# Linkedin: www.linkedin.com/alessandromirandagoncalves #
# Abril/2022                                            #
#########################################################
# Programa irá exportar para o Postgresql a partir de um arquivo CSV
# O cabeçalho (header) da planilha deve conter os nomes dos campos
# a serem exportados

import pandas as pd         # Biblioteca com funções de ETL
import pandera as pa        # Biblioteca com funções de ETL
import sys                  # Biblioteca com funções de sistema
import psycopg2 as sql      # Permite manipulação de dados Postgres
import datetime             # para cálculos de tempo usado pelo programa


def imprimir_cabecalho():  # Exibe informações iniciais do programa
    print(58*'-')
    print('Programa exportador CSV -> Postgresql')
    print(58*'-')

def conectar_banco():  # Conecta ao banco de dados e deixa aconexão aberta em "conexao"
    try:
        print('Conectando com Postgresql')
        # Credenciais para conexão
        database_username = 'teste'
        database_password = 'teste'
        database_ip = '127.0.0.1'
        database_name = 'bdteste'
        # Primeiramente se conecta ao Mysql
        conexao = sql.connect(host='localhost',
                               database='dbteste',
                               user='postgres',
                               password='teste')
        print('Conexão com SGBD com sucesso.')
    except sql.DatabaseError as e:
        print('*** ERRO: Não foi possível conectar ao banco {} no servidor {} porta:5432.'.format(database_name,database_ip))
        sys.exit(0)
    return conexao

# Executa a abertura do arquivo csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_csv(nome_arquivo):
    try:
        print('Lendo arquivo ', nome_arquivo)
        valores_ausentes=['','**','***','****','*****','###!','####','NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_planilha = pd.read_csv(nome_arquivo,encoding='ISO-8859-1',sep=';',dayfirst=True,na_values=valores_ausentes,)
        print('Arquivo lido com sucesso')
    #Erro do arquivo não existir
    except FileNotFoundError as e:
        print('*** ERRO: Arquivo ' + nome_arquivo +'.csv não encontrado. Favor verificar.')
        sys.exit()
    #Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_planilha

# Verifica se o arquivo tem as colunas nos formatos corretos
# Se não, mostra erro e encerra o programa
def validar_arquivo(df_planilha):
    try:
        print('Validando arquivo ocorrencia...')
        schema = pa.DataFrameSchema(
            columns={"codigo_ocorrencia": pa.Column(pa.Int,nullable=True),
                     "codigo_ocorrencia2": pa.Column(pa.Int),
                     "ocorrencia_classificacao": pa.Column(pa.String),
                     "ocorrencia_cidade": pa.Column(pa.String),
                     "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2, 2),nullable=True),
                     "ocorrencia_aerodromo": pa.Column(pa.String, nullable=True),
                     "ocorrencia_dia": pa.Column(pa.DateTime),
                     "ocorrencia_hora": pa.Column(pa.String,
                                                  pa.Check.str_matches(r'^([0-1][0-9]|[2][0-3])(:([0-5][0-9])){1,2}$'),
                                                  nullable=True),
                     "total_recomendacoes": pa.Column(pa.Int)
                     }
        )
        schema.validate(df_planilha,lazy=True)
        print('Arquivo validado com sucesso')
    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)    # erros de dataframe ou schema
        print(e.data)             # dataframe inválido
        print(58 * '-')
        sys.exit()

#exporta dados do dataframe para a tabela informada
def exportar(conexao,df_planilha,nome_arquivo):
    print('Gerando tabela ' + nome_arquivo + '...')
    # Convert dataframe to sql table
    # Extrai os nomes dos campos (linha 1)
    Query = "Insert into " +nome_arquivo

    for index, rows in df_planilha.iterrows():
        # Create list for the current row
        #my_list = [rows.Date, rows.Event, rows.Cost]
        print(rows[0])

    df_planilha.to_sql('ocorrencias', conexao, index=False)
    print('Tabela gerada.')

if __name__ == "__main__":
    tempo_inicial = datetime.datetime.now()
    imprimir_cabecalho()
    nome_arquivo = "teste.csv"

    df_planilha = abrir_arquivo_csv(nome_arquivo)
    ## validar_arquivo_ocor(df_planilha) # Caso queira validar
    conexao = conectar_banco()
    exportar(conexao,df_planilha,"clientes")

    tempo_final = datetime.datetime.now()
    tempo_total = tempo_final-tempo_inicial
    print("\nTempo total transcorrido (em s): {}".format(tempo_total))