import pandas as pd
import asyncio
import aiohttp
from db_connection import create_connection, insert_contatos_query, select_origin_query
import warnings
import os
import time
from dotenv import load_dotenv, find_dotenv
warnings.filterwarnings('ignore')

# Inciar as variáveis de ambiente
load_dotenv(find_dotenv())
SECRET_DB_PASSWORD = os.environ.get('SECRET_DB_PASSWORD')
SECRET_API_KEY = os.environ.get('SECRET_API_KEY')

class ContatosAsyncIo:
    def __init__(self, SECRET_API_KEY, n):
        self.headers = {'token': f'{SECRET_API_KEY}',
                        'Content-Type': 'application/json'}
        self.pagina = [i for i in range(n, n+100)] ###### NUMERO MÁGICO QUE DEVE SER ATUALIZADO ######
        self.nome = ''
        self.cnpjCpf = ''
        self.fone = ''
        self.email = ''
        self.results = []
        self.df_contatos = pd.DataFrame()

    def get_tasks(self, session):
        tasks = []
        for pagina in self.pagina:
            tasks.append(
                session.get(url = f'https://api.crmsimples.com.br/API?method=getListContato&pagina={pagina}&nome={self.nome}&cnpjCpf={self.cnpjCpf}&fone={self.fone}&email={self.email}',
                            headers=self.headers,
                            ssl=False))
        return tasks

    async def get_list_contato (self):
        async with aiohttp.ClientSession() as session:
            tasks = ContatosAsyncIo.get_tasks(self, session)
            responses = await asyncio.gather(*tasks)
            for r in responses:
                requested_id = await r.json()
                self.results.append(requested_id)

    def convert_to_df (self):
        try:
            for r in self.results:
                df_atual = pd.json_normalize(r['Result'])
                self.df_contatos = pd.concat([self.df_contatos, df_atual])
        except Exception as e:
            print(r)
            print('Erro devido a:', e)
        finally:
            return self.df_contatos

def InsertDiferentContacts(SECRET_API_KEY, SECRET_DB_PASSWORD, NUMBER_OF_INTERATIONS):
    # Numero de paginas a serem requisitadas na API
    n = 1
    for interation in range(NUMBER_OF_INTERATIONS):
        # Incializa a classe de requisição assíncrona
        contatos = ContatosAsyncIo(SECRET_API_KEY, n)
        asyncio.run(contatos.get_list_contato())

        # Verficar se os ids requisitados já estão registrados no banco
        df_contatos_final = contatos.convert_to_df()

        # Conexão com a tabela CONTATOS do banco
        con = create_connection('CRM.db', SECRET_DB_PASSWORD)
        cursor = con.cursor()
        df_contatos_banco = select_origin_query(con=con, table='CONTATOS')

        # Diferença entre os dados
        id_requisitado = set(df_contatos_final['Id'])
        id_banco = set(df_contatos_banco['contatoidinterno'])
        dif = id_requisitado - id_banco

        # DataFrame final apenas com os ids diferentes
        df_contatos_final = df_contatos_final[df_contatos_final['Id'].isin(dif)]
        novos_ids = len(df_contatos_final)
        df_contatos_final['indice'] = [index for index in range(novos_ids)]
        df_contatos_final = df_contatos_final[['indice','Id','Nome','Cidade']]
        sql = [i for i in df_contatos_final.to_numpy()]

        # Inserir dados faltantes na tabela CONTATOS
        try:
            insert_contatos_query(sql, con, cursor)
            print(f'Geração {n}: Com total de {novos_ids} novas linhas a serem inseridas no banco')
            con.close()
            time.sleep(3)
            # Muda o numero de paginas a serem requisitadas 
            n = n + 100

        except Exception as e:
            print('Não foi possível concluir a operação devido a:', e)

InsertDiferentContacts(SECRET_API_KEY, SECRET_DB_PASSWORD, NUMBER_OF_INTERATIONS = 7)