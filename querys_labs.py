import pandas as pd
from sys import path
import models
from sqlmodel import delete, Session

path.append(r'C:\Program Files (x86)\Microsoft.NET\ADOMD.NET\160')

from pyadomd import Pyadomd

def query_and_update(model:models.SQLModel, query: str, servidor: str, banco_de_dados: str):
    constr= f'Provider=MSOLAP;Data Source={servidor};Catalog={banco_de_dados};'

    drop = {
        'Cliente': '[volume]',
        'Produto': '[volume]'
    }

    with Pyadomd(constr) as con:
        with con.cursor().execute(query) as cur:
            df = pd.DataFrame(cur.fetchall(), columns = [i.name for i in cur.description])
            
            df = df.drop(columns=drop.get(model.__name__, []))

            df.columns = list(model.model_fields.keys())

            #df.to_csv(f'bases/{name}.csv', sep=';')

            df.to_sql(model.__name__.lower(), models.engine, if_exists='replace', index=False)

            Session(models.engine).commit()
            

if __name__ == '__main__':

    #servidor = 'asazure://aspaaseastus2.asazure.windows.net/lab'
    #banco_de_dados = 'lab_light'

    servidor = 'asazure://aspaaseastus2.asazure.windows.net/lab2'
    banco_de_dados = 'lab'
    
    query_cliente = '''
    EVALUATE
SUMMARIZECOLUMNS(
    Cliente[8.1 Código UNB & Código PDV],
    Cliente[Geo],
    Cliente[Código UNB],
    Cliente[4. Código PDV],
    Cliente[Comercial],
    Cliente[Operação],
    Cliente[Tipo Operação],
    Cliente[Código GV],
    Cliente[5. Código Setor],
    Cliente[Latitude],
    Cliente[Longitude],
    Cliente[Nome Segmento],
    Cliente[Segmentação Primária],
    Cliente[Nome PDV],
    Cliente[Documento],
    Cliente[Status PDV],
   "volume", CALCULATE([1. Volume (hl) Real], 'Período'[Referência M0] > -7)
)
'''

    query_produto = '''
    EVALUATE
SUMMARIZECOLUMNS(
    Produto[Código Abreviado Produto],
    Produto[Nome Abrev. SKU],
    Produto[Nome SKU],
    Produto[Nome Tipo Marca Produto],   
    Produto[Flag Market Place],
    Produto[Fator HL],
    Produto[Nome Marca SKU],
    Produto[Nome Linha Embalagem Consolidado],
    KEEPFILTERS( TREATAS( {2022, 2023, 2024}, 'Período'[Ano] )),KEEPFILTERS( TREATAS( {"CO"}, Cliente[Geo] )),
    "volume", [1. Volume (hl) Real])
'''

    query_cesta = '''
EVALUATE
SUMMARIZECOLUMNS(
    AuxProdutoCesta[IdCesta],
    AuxProdutoCesta[Código Sku Abrev.],
    AuxProdutoCesta[Nome Cesta],
    AuxProdutoCesta[Nome Cesta Abrev.]
)
ORDER BY 
    AuxProdutoCesta[IdCesta] ASC,
    AuxProdutoCesta[Código Sku Abrev.] ASC,
    AuxProdutoCesta[Nome Cesta] ASC,
    AuxProdutoCesta[Nome Cesta Abrev.] ASC
'''

    

    #query_and_update(model=models.Cliente, query=query_cliente, servidor=servidor, banco_de_dados=banco_de_dados)
    #query_and_update(model=models.Produto, query=query_produto, servidor=servidor, banco_de_dados=banco_de_dados)
    query_and_update(model=models.Cesta, query=query_cesta, servidor=servidor, banco_de_dados=banco_de_dados)
    
#     for geo in ['CO', 'NO', 'NE', 'SUL', 'MG', 'SP', 'RJ/ES']:
#         for ano in [2022, 2023]:
#             for mes in [1,2,3,4,5,6,7,8,9,10,11,12]:
#                 if geo == 'RJ/ES':
#                     nome = f'base_RJ_{ano}_{mes}'
#                 else:
#                     nome = f'base_{geo}_{ano}_{mes}'

#                 from glob import glob

#                 nomes = glob('*', root_dir='bases')

#                 if glob(f'{nome}.csv', root_dir='bases'):
#                     print(f'{nome} - já existe')
#                     continue
                
#                 print(f'{nome}')

#     #             query_volume_brasil = f'''
#     #         EVALUATE
#     #     SUMMARIZECOLUMNS(
#     #         Cliente[Geo],
#     #         Cliente[8.1 Código UNB & Código PDV],
#     #         'Período'[Ano],
#     #         'Período'[Mês],
#     #         'Período'[Dia],
#     #         Cliente[Comercial],
#     #         Cliente[Operação],
#     #         Cliente[Código UNB],
#     #         Cliente[4. Código PDV],
#     #         Cliente[Sigla UF],
#     #         Produto[Código Abreviado Produto],
#     #         Produto[Nome SKU],
#     #         KEEPFILTERS( TREATAS( {{ {ano} }}, 'Período'[Ano] )),
#     #         KEEPFILTERS( TREATAS( {{ {1} }}, 'Período'[Mês] )),
#     #         KEEPFILTERS( TREATAS( {{ "NAB" }}, 'Cesta Produto'[Nome Cesta Abrev.] )),
#     #         KEEPFILTERS( TREATAS( {{ "{geo}" }}, Cliente[Geo] )),
#     #         "1. Volume (hl) Real", [1. Volume (hl) Real],
#     #         "Faturamento (R$)", [Faturamento (R$)]
#     #     )
#     #     '''
                    
#                 vol_br_agrup = f'''
#                     EVALUATE
# SUMMARIZECOLUMNS(
#     Cliente[Geo],
#     Cliente[8.1 Código UNB & Código PDV],
#     'Período'[Ano],
#     'Período'[Mês],
#     Cliente[Comercial],
#     Cliente[Operação],
#     Cliente[Código UNB],
#     Cliente[4. Código PDV],
#     KEEPFILTERS( TREATAS( {{{ano}}}, 'Período'[Ano] )),
#     KEEPFILTERS( TREATAS( {{{mes}}}, 'Período'[Mês] )),
#     KEEPFILTERS( TREATAS( {{ "NAB" }}, 'Cesta Produto'[Nome Cesta Abrev.] )),
#     KEEPFILTERS( TREATAS( {{"{ geo }"}}, Cliente[Geo] )),
#     "1. Volume (hl) Real", [1. Volume (hl) Real],
#     "volume single", calculate([1. Volume (hl) Real], 'Cesta Produto'[Nome Cesta Abrev.] = "SINGLE CSD"),
#     "volume agua", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "AGUA"),
#     "volume cha", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "CHA"),
#     "volume comp", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "COMPLEMENTO ALIMENTAR"),
#     "volume energetico", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "ENERGETICO"),
#     "volume isotonico", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "ISOTONICO"),
#     "volume refresco", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "REFRESCO"),
#     "volume refri", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "REFRIGERANTE"),
#     "volume suco", calculate([1. Volume (hl) Real], Produto[Nome Tipo Marca Produto] = "SUCO"),
#     "distribuição", [# SKUs PDVs (Distribuição)]
#    )
#     '''         
                
#                 query_and_update(model=models.Cliente, query=vol_br_agrup, servidor=servidor, banco_de_dados=banco_de_dados, name=nome)    