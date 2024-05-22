import pyodbc
import pandas as pd

import models
from sqlmodel import delete, Session

def query_and_update_brewdat(model:models.SQLModel, query: str, servidor: str, banco_de_dados: str):
    con = pyodbc.connect(
    r'DRIVER={ODBC Driver 18 for SQL Server};'
    f'SERVER={servidor};'
    f'DATABASE={banco_de_dados};'
    r'Authentication=ActiveDirectoryInteractive;'
    r'TrustedConnection=yes;'
    r'UID=99810903@ambev.com.br;'
    )

    
    with con.cursor().execute(query) as cur:

        df = pd.DataFrame([list(x) for x in list(cur)], columns = list(model.model_fields.keys()))

        df.to_sql(model.__name__.lower(), models.engine, if_exists='replace', index=False)

        Session(models.engine).commit()
            


if __name__ == '__main__':
    servidor = 'edhazurencertdmgbprod.database.windows.net'
    banco_de_dados = 'edhazurencertdmgbprod'
    
    query_nf = """
select 

cast(c.cod_unb as varchar) + '_' + cast(nf.cod_pdv as varchar) as chave,
c.cod_unb,
nf.cod_pdv,
nf.cod_prod,
sum(nf.val_qtd_total) as volume,
sum(nf.val_qtd_boni_desin) as volume_boni,
sum(nf.ttv_politica_plan*nf.val_qtd_total) as ttv_plan,
sum(nf.ttv_real*nf.val_qtd_total) as ttv_real,
sum(nf.ttv_politica_plan*nf.val_qtd_total - nf.ttv_real*nf.val_qtd_total) as dispersao,
sum(nf.ttv_politica_plan*nf.val_qtd_total - nf.ttv_real*nf.val_qtd_total)/sum(nf.ttv_politica_plan*nf.val_qtd_total) as percentual_desc

from abi_algoselling_stg.vc_aderencia_nf_f_stg as nf
join abi_algoselling_stg.vc_depara_d_stg as c on nf.cod_cliente = c.cod_cliente

WHERE 

c.cod_cliente in (40, 139, 355, 740, 872, 10700, 29537, 32352, 36038, 37646, 49535, 54264, 59971, 60074, 60663, 61333, 61367, 61377, 61913, 63785, 66926, 66961, 69800, 70313, 70335, 72400, 73400, 74179, 82056, 82473, 84127, 84222, 84284, 84384, 85789, 86462, 86855, 86912, 87026, 87091, 87456, 87457, 87477, 87478, 87814, 87855, 87892, 88019, 89672, 89706, 89733, 89734, 90521, 92013, 93785, 95278, 95630, 97075, 98218, 98728, 99059, 99245, 99246, 99597, 101156, 101626, 101703, 101839, 112000, 113100, 132000, 158200, 171600, 186300)
and cast(left(cast(nf.dat_emissao_nf as char),4) as INTEGER) = year(getutcdate())
and cast(right(left(cast(nf.dat_emissao_nf as char),6),2) as INTEGER) >= month(getutcdate())-3

group by

c.cod_unb,
nf.cod_pdv,
nf.cod_prod
"""

    query_and_update_brewdat(models.NotaFiscal, query_nf, servidor, banco_de_dados)
