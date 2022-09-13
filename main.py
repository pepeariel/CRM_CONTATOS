from db_connection import create_connection, insert_origin_query, select_origin_query
from get_fonte import GetFonteContato
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import os

# Inciar as variáveis de ambiente
load_dotenv(find_dotenv())
SECRET_DB_PASSWORD = os.environ.get('SECRET_DB_PASSWORD')
SECRET_API_KEY = os.environ.get('SECRET_API_KEY')

# Tokens de acesso a API do CRM -- precisa ser escondido!!
headers = {
  'token': f'{SECRET_API_KEY}',
  'Content-Type': 'application/json'
}

if __name__ == '__main__':

    # Faz a requisição na API e retorna o Dataframe desejado
    df_origem = GetFonteContato(headers=headers)
    sql = [i for i in df_origem.to_numpy()]

    # Cria a conexão com o Banco de dados na nuvem
    con = create_connection('CRM.db', SECRET_DB_PASSWORD)
    cursor = con.cursor()

    # Insere os dados no banco
    # insert_origin_query(sql, con, cursor)
    df = select_origin_query(con, table='ORIGEM')
    print(df)