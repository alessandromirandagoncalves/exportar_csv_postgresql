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
import csv
from csv import DictReader
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

# Função para inserir dados no banco
def inserir_db(con,sql):
    cur = con.cursor()
    try:
        cur.execute(sql)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("** ERROR: %s" % error)
        con.rollback()
        cur.close()
        return 1
    cur.close()

#exporta dados do dataframe para a tabela informada
def exportar(conexao,nome_tabela):
    try:
        print('Lendo arquivo ', nome_tabela)
        with open(nome_tabela+'.csv','r') as csv_file:
            planilha2 = csv.reader(csv_file, delimiter=';',quotechar="'")
            planilha = DictReader(csv_file)
            print('Gerando tabela ' + nome_tabela + '...')

            # Extrai os nomes dos campos (linha 1)
            campos = ''.join(planilha.fieldnames)
            campos = campos.replace(";",",")
            qt_colunas = campos.count(',') + 1
            valores = " values "
            for row in planilha2:
                valor = ""
                valores = valores + "("
                for coluna in range(qt_colunas):
                    if (''.join(row[coluna]) == ''):
                        valor = valor + 'Null, '
                    else:
                        valor = valor + "'" + "".join(row[coluna]) + "', "
                valores = valores + valor
                valores = valores.replace(", )",")")
                valores = valores + "),"

            valores = valores.replace(", )", ")")
            valores = valores[:len(valores)-1]
            insert = "Insert into " + nome_tabela + " (" + campos + ")" + valores

    #Erro do arquivo não existir
    except FileNotFoundError as e:
        print('*** ERRO: Arquivo ' + nome_tabela +'.csv não encontrado. Favor verificar.')
        sys.exit()

    #Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()

    print('Registros incluídos com sucesso')

    return insert

if __name__ == "__main__":
    tempo_inicial = datetime.datetime.now()
    imprimir_cabecalho()

    ##planilha = abrir_arquivo_csv(nome_tabela)
    ## validar_arquivo_ocor(planilha) # Caso queira validar
    conexao = conectar_banco()
    insert = exportar(conexao,"clientes")
    retorno = inserir_db(conexao,insert)
    if (retorno == 1):
        sys(0)

    tempo_final = datetime.datetime.now()
    tempo_total = tempo_final-tempo_inicial
    print("\nTempo total transcorrido (em s): {}".format(tempo_total))