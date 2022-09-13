import pandas as pd
import requests


# Função que faz a requisição da origem dos contatos na API do CRM (ex: site, ERP, redes sociais...)
def GetFonteContato(headers):
    request_fonte_contato = requests.request(method = 'GET',
            url = 'https://api.crmsimples.com.br/API?method=getFonteContato',
            headers = headers)
    #  Transforma a req Json em Dataframe com 2 colunas -> [id,Descricao]
    df = pd.json_normalize(request_fonte_contato.json())
    # Renomeia a coluna com as origens dos contatos para -> Origem
    df = df.rename(columns={'Descricao': 'Origem'})
    # Atribui aos campos os formatos corretos
    df['Id'] = df['Id'].astype(int)
    return df

# IdUsuarioInclusao -- precisa voltar a ser incluido no banco  
