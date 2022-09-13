import psycopg2
import pandas as pd

# Conexão ao banco de dados CRM na AWS
def create_connection(db_file, SECRET_DB_PASSWORD):
    try:
        con = psycopg2.connect(
        host = 'database-1.c7xrnorluyxo.us-east-1.rds.amazonaws.com',
        port = '5432',
        user = 'postgres',
        password = f'{SECRET_DB_PASSWORD}')
  
    except Exception as erro:
        print('Erro ao conectar:', erro)
    
    return con

def insert_origin_query(sql, con, cursor):
  print('Inserindo Dados...')

  query = """ INSERT INTO ORIGEM (
              id, origem)

              VALUES (%s, %s)"""

  cursor.executemany(query, sql)
  con.commit()
  con.close()

  return print('Dados Inseridos na Tabela Produtos com sucesso!')

def select_origin_query(con, table):
    query = f"SELECT contatoidinterno FROM {table}"
    df = pd.read_sql(query,con)
    #con.close()

    return df

def insert_contatos_query(sql, con, cursor):
  query = """ INSERT INTO CONTATOS (
              id, contatoidinterno, nome, cidade)

              VALUES (%s, %s, %s, %s)"""

  cursor.executemany(query, sql)
  con.commit()
  print('Dados inseridos com sucesso!')
  con.close()