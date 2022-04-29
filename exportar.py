#########################################################
# Alessandro Miranda Gonçalves                          #
# Linkedin: www.linkedin.com/alessandromirandagoncalves #
# Abril/2022                                            #
#########################################################
# Programa irá exportar para o Postgresql a partir de um arquivo CSV
# O cabeçalho (header) da planilha deve conter os nomes dos campos
# a serem exportados

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
        # Primeiramente se conecta ao banco
        conexao = sql.connect(host='localhost',
                               database='dbteste',
                               user='postgres',
                               password='teste')
        print('Conexão com SGBD com sucesso.')
    except sql.DatabaseError as e:
        print('*** ERRO: Não foi possível conectar ao banco {} no servidor {} porta:5432.')
        sys.exit(0)
    return conexao

# Função para inserir dados no banco
def inserir_db(con,query):
    cur = con.cursor()
    try:
        cur.execute(query)
        con.commit()
    except (Exception, sql.DatabaseError) as error:
        print("** ERROR: %s" % error)
        con.rollback()
        cur.close()
        sys.exit(0)
    cur.close()

#exporta dados do dataframe para a tabela informada
def exportar(conexao,nome_tabela):
    try:
        print('Lendo arquivo ', nome_tabela)
        with open(nome_tabela+'.csv','r', encoding='ISO-8859-1') as csv_file:
            planilha2 = csv.reader(csv_file, delimiter=';',quotechar="'")
            planilha = DictReader(csv_file)
            print('Inserindo na tabela ' + nome_tabela + '...')

            # Extrai os nomes dos campos (linha 1)
            campos = ''.join(planilha.fieldnames)
            campos = campos.replace(";",",")
            qt_colunas = campos.count(',') + 1
            valores = " values "
            registros = 0
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
                registros+=1

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

    print(registros,' registros incluídos com sucesso')

    return insert

if __name__ == "__main__":
    tempo_inicial = datetime.datetime.now()
    imprimir_cabecalho()

    ##planilha = abrir_arquivo_csv(nome_tabela)
    ## validar_arquivo_ocor(planilha) # Caso queira validar
    conexao = conectar_banco()
    insert = exportar(conexao,"clientes")
    inserir_db(conexao,insert)

    tempo_final = datetime.datetime.now()
    tempo_total = tempo_final-tempo_inicial
    print("\nTempo total transcorrido (em s): {}".format(tempo_total))