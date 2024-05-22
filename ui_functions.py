import streamlit as st
from models import SQLModel, Session, engine, Cesta, Produto, NotaFiscal, Cliente, Tabelas_Especiais, Session, Solicitação_Tabela_Especial, Solicitação_Segmento, select
import pandas as pd
import polars as pl
from datetime import datetime

from supabase import create_client, Client
supabase: Client = create_client(st.secrets.connections['supabase'].get('SUPABASE_URL'), st.secrets.connections['supabase'].get('SUPABASE_KEY'))

from PIL import Image as PILImage
import io
import base64


def get_skus_cesta(cesta: str):
    with Session(engine) as session:
        query = select(Cesta.cod_sku).where(Cesta.nome_cesta_abrev == cesta).distinct()
        session.exec(query)
        results = session.exec(query).fetchall()
    return results


def get_tabelas_solictações_por_chave(tabela: SQLModel, campo: SQLModel, chave: list) -> pl.LazyFrame:
    query = select(tabela).where(campo.in_(chave))
    with Session(engine) as session:
        results = list(session.exec(query).fetchall())
        return pl.from_records(results).lazy()

def convert_image(string: str) -> str:
    image = PILImage.open(string)
    output = io.BytesIO()    
    image.save(output, format='PNG')
    encoded_string = "data:image/png;base64,"+base64.b64encode(output.getvalue()).decode()
    return encoded_string

def ajuste_image(string: str) -> str:
   return "data:image/jpeg;base64,"+string


tipo_solicitações = [
    'Ajuste de Segmento',
    'Inclusão/Exclusão PDV de Tabela Especial',
    'Ação de Cobertura',
    'Bonificação',
    'Inclusão SKU em Ambec',
    'Cadastro TTV de novo SKU'
    ]

@st.cache_data
def lista_comerciais(chave: str):
    with Session(engine) as session:
        statement = select(Cliente.comercial).distinct().order_by(Cliente.comercial)
        return list(session.exec(statement))

@st.cache_data
def lista_comerciais():
    with Session(engine) as session:
        statement = select(Cliente.comercial).distinct().order_by(Cliente.comercial)
        return list(session.exec(statement))

@st.cache_data
def lista_operações(comercial):
    with Session(engine) as session:
        statement = select(Cliente.operação).where(Cliente.comercial == comercial).order_by(Cliente.operação).distinct()
        return list(session.exec(statement))
    
@st.cache_data
def lista_tabelas(operação):
    with Session(engine) as session:
        statement = select(Cliente.unb).where(Cliente.operação == operação).distinct()
        unb = session.exec(statement).first()
        statement = select(Tabelas_Especiais.nome_tabela).where(Tabelas_Especiais.unb == unb).order_by(Tabelas_Especiais.nome_tabela).distinct()
        return list(session.exec(statement))
    
@st.cache_data
def depara_operação_unb() -> pl.DataFrame:
    with Session(engine) as session:
        statement = select(Cliente.comercial, Cliente.operação, Cliente.unb).distinct()
        result = list(session.exec(statement).fetchall())
        data = [{'comercial': x[0], 'operação': x[1], 'unb': x[2]} for x in result]
        return pl.DataFrame(data)
    
def ajuste_segmento(ss, n_acao, col1, col2, col3, col4):
    with col1:
        st.write('Baixe o template clicando no botão abaixo:')
        st.download_button('Baixar o template', pd.DataFrame({'unb':[999,999,999], 'cod_pdv':[1,2,3], 'segmento': ['rota', 'asr', 'centro']}).to_csv(index=False, sep=';').encode('ISO-8859-15'), mime='text/csv', file_name='template_segmento.csv', key=f'download_template_{n_acao}')
    with col2:
        st.file_uploader('Selecione o template', ['csv'], False, key=f'upload_template_{n_acao}')
        st.write('')
    with col3:

        if ss[f'upload_template_{n_acao}']:
            base_ação = pd.read_csv(ss[f'upload_template_{n_acao}'], sep=';')
            base = pl.from_pandas(base_ação).filter(pl.col.unb.is_not_null()).group_by('segmento').agg([
                pl.col.cod_pdv.count().alias('qnt')
            ])
            st.data_editor(base.to_pandas(), hide_index=True)
        else:
            st.write('')
            
def tabela_especial(ss, n_acao, col1, col2, col3, col4):
    with col1:
        st.number_input('Código PDV', 0, 999999, step=1, key=f'pdv_especial_{n_acao}')
    with col2:
        st.selectbox('Tabela', lista_tabelas(ss[f'select_operação_{n_acao}']), key=f'tabela_especial_{n_acao}')
    with col3:
        st.radio('Ação', ['Incluir', 'Excluir'], horizontal=True, key=f'acao_especial_{n_acao}')
    
    

def make_forms(ss, n_acao):    
    with st.expander('nome_ação', True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.selectbox('Tipo de Solicitação', tipo_solicitações, 0, key=f'select_tipo_solic_{n_acao}')
        with col2:
            st.selectbox('Comercial', lista_comerciais(), key=f'select_comercial_{n_acao}')
        with col3:
            st.selectbox('Operação', lista_operações(ss[f'select_comercial_{n_acao}']), key=f'select_operação_{n_acao}')
        with col4:
            st.markdown('**Faça o Upload do print do dono do pote:**')
            st.file_uploader("Print do OK", ['png', 'jpg', 'jpeg', 'gif'], key=f'print_ok_{n_acao}')
        match ss.get(f'select_tipo_solic_{n_acao}', 'nok'):
            case 'Ajuste de Segmento':
                ajuste_segmento(ss, n_acao, col1, col2, col3, col4)
            case 'Inclusão/Exclusão PDV de Tabela Especial':
                tabela_especial(ss, n_acao, col1, col2, col3, col4)
            case 'Inclusão SKU em Ambec':
                st.write(tipo_solicitações[2])
            case 'Cadastro TTV de novo SKU':
                st.write(tipo_solicitações[3])
            case 'nok':
                st.write('Solicitação Inexistente') 



def salvar_ações(ss):
    n = ss['n_ação']
    session = Session(engine) 

    data = datetime.now()

    for n_acao in range(0,n):
        match ss.get(f'select_tipo_solic_{n_acao}', 'nok'):
            case 'Ajuste de Segmento':

                format = ss[f'print_ok_{n_acao}'].name.split('.')[1]

                supabase.storage.from_('Prints').upload(f'Prints/segmento_{data}_{n_acao}.{format}', ss[f'print_ok_{n_acao}'].getvalue())
                
                base = pl.read_csv(ss.get(f'upload_template_{n_acao}'), separator=';').filter(pl.col.unb.is_not_null())
                base = base.join(depara_operação_unb(), on='unb', how='left').with_columns([
                    pl.lit(ss['email_solicitante']).alias('email_usuario'),    
                    pl.lit(ss['nome_solicitante']).alias('nome_usuario'),
                    pl.lit('CO').alias('geo'),
                    # pl.lit(ss.get(f'select_comercial_{n_acao}')).alias('comercial'),
                    # pl.lit(ss.get(f'select_operação_{n_acao}')).alias('operação'),
                    pl.lit(f'Prints/segmento_{data}_{n_acao}.{format}').alias('print_aprovação'),
                ]).to_pandas()

                st.write(base)

                incluir = []
                for record in base.convert_dtypes().to_records(index=False):
                    dict = {base.columns[i] : x for i, x in enumerate(record)}
                    incluir.append(Solicitação_Segmento(**dict))
                
                session.add_all(incluir)

            case 'Inclusão/Exclusão PDV de Tabela Especial':

                format = ss[f'print_ok_{n_acao}'].name.split('.')[1]

                supabase.storage.from_('Prints').upload(f'Prints/segmento_{data}_{n_acao}.{format}', ss[f'print_ok_{n_acao}'].getvalue())

                solicitação = Solicitação_Tabela_Especial(
                    nome_usuario = ss['nome_solicitante'],
                    email_usuario = ss['email_solicitante'],
                    geo = 'CO',
                    comercial = ss.get(f'select_comercial_{n_acao}'),
                    operação = ss.get(f'select_operação_{n_acao}'),
                    cod_pdv = ss.get(f'pdv_especial_{n_acao}'),
                    tabela = ss.get(f'tabela_especial_{n_acao}'),
                    ação = ss.get(f'acao_especial_{n_acao}'),
                    print_aprovação = f'Prints/segmento_{data}_{n_acao}.{format}'
                )

                session.add(solicitação)

            case 'Inclusão SKU em Ambec':
                pass
            case 'Cadastro TTV de novo SKU':
                pass
            case 'nok':
                st.write('Solicitação Inexistente')
    
    session.commit()
            
def make_aprovação_tabelas_especiais(operações: list, solicitantes: list):
    statement = select(
        Solicitação_Tabela_Especial
        ).where(
            Solicitação_Tabela_Especial.status == 'Nova Solicitação'
        ).where(
            Solicitação_Tabela_Especial.operação.in_(operações)
        ).where(
            Solicitação_Tabela_Especial.nome_usuario.in_(solicitantes)
        )
     
    with Session(engine) as session:
        results = list(session.exec(statement).fetchall())
    
    base = pl.DataFrame([r.model_dump() for r in results]).lazy()

    base = base.join(depara_operação_unb().lazy(), on='operação', how='left')
    base = base.with_columns(
            pl.concat_str([pl.col.unb, pl.col.cod_pdv], separator='_').alias('chave'),
            pl.col('print_aprovação').apply(supabase.storage.from_('Prints').get_public_url, return_dtype=pl.Utf8).alias('print_aprovação'),
            pl.lit(True).alias('Aprovar')
    )

    chaves_solicitadas = base.select(pl.col.chave.unique()).collect().to_series().to_list()

    cliente = get_tabelas_solictações_por_chave(Cliente, Cliente.chave, chaves_solicitadas)

    nf = get_tabelas_solictações_por_chave(NotaFiscal, NotaFiscal.chave, chaves_solicitadas)

    produtos_solicitados = nf.select(pl.col.cod_sku.unique()).collect().to_series().to_list()
    
    produtos = get_tabelas_solictações_por_chave(Produto, Produto.cod_sku, produtos_solicitados)
    
    nf = nf.join(produtos, on = 'cod_sku', how = 'left').with_columns([
        (pl.col.volume * pl.col.fator_hl).alias('volume'),
        (pl.col.volume_boni * pl.col.fator_hl).alias('volume_boni')
    ]).group_by([pl.col.chave]).agg([
        pl.col.volume.sum().alias('volume_tt'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('CERVEJA'))).sum().alias('volume_cerveja'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('NAB'))).sum().alias('volume_nab'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('MATCH'))).sum().alias('volume_match'),
        pl.col.volume_boni.sum(),
        pl.col.ttv_plan.sum(),
        pl.col.ttv_real.sum(),
    ]).with_columns([
        (pl.col.volume_boni / pl.col.volume_tt).alias('% Boni'),
        (1-(pl.col.ttv_real / pl.col.ttv_plan)).alias('% Desc'),
    ])
    
    #st.data_editor(produtos.to_pandas())

    base = base.join(
        cliente, on='chave', how='left'
    ).join(
        nf, on = 'chave', how='left'
        ).select([
            'Aprovar', 'data_inclusão', 'chave', 'geo', 'comercial', 'operação', 'cod_pdv', 'nome_fantasia',
            'tabela', 'ação', 'NGE', 'segmentação_primária', 'volume_cerveja', 'volume_nab', 'volume_match', '% Boni', '% Desc', 'print_aprovação', 'id'
        ]).with_columns(pl.col(x)*100 for x in ['% Desc', '% Boni'])

    st.data_editor(base.collect().to_pandas(), hide_index=True, column_config = {
        'id': st.column_config.NumberColumn(disabled=True),
            'Aprovar': st.column_config.CheckboxColumn(),
            'print_aprovação': st.column_config.ImageColumn('Print do OK'),
            'segmentação_primária': 'Seg Primária',
            'data_inclusão': st.column_config.DateColumn('Data', format='DD/MM/YYYY'),
            'volume_cerveja': st.column_config.NumberColumn('Vol Cerveja', default='float', format='%.2f'),
            'volume_nab': st.column_config.NumberColumn('Vol NAB', default='float', format='%.2f'),
            'volume_match': st.column_config.NumberColumn('Vol Match', default='float', format='%.2f'),
            'cod_pdv': 'PDV',
            'segmento': 'Seg Solicitado',
            'nome_fantasia': 'Nome Fantasia',
            'operação_right': 'Operação',
            'comercial_right': 'Comercial',
            '% Desc': st.column_config.NumberColumn('% Desc L3M', default='float', format='%.2f %%'),
            '% Boni': st.column_config.NumberColumn('% Boni L3M', default='float', format='%.2f %%')
    })

def make_aprovação_segmento(operações: list, solicitantes: list):
    statement = select(
        Solicitação_Segmento
        ).where(
            Solicitação_Segmento.status == 'Nova Solicitação'
        ).where(
            Solicitação_Segmento.operação.in_(operações)
        ).where(
            Solicitação_Segmento.nome_usuario.in_(solicitantes)
        )
     
    with Session(engine) as session:
        results = list(session.exec(statement).fetchall())
    
    base = pl.DataFrame([r.model_dump() for r in results]).lazy()

    base = base.join(depara_operação_unb().lazy(), on='operação', how='left')
    base = base.with_columns(
            pl.concat_str([pl.col.unb, pl.col.cod_pdv], separator='_').alias('chave'),
            pl.col('print_aprovação').apply(supabase.storage.from_('Prints').get_public_url, return_dtype=pl.Utf8).alias('print_aprovação'),
            pl.lit(True).alias('Aprovar')
    )

    chaves_solicitadas = base.select(pl.col.chave.unique()).collect().to_series().to_list()

    cliente = get_tabelas_solictações_por_chave(Cliente, Cliente.chave, chaves_solicitadas)

    nf = get_tabelas_solictações_por_chave(NotaFiscal, NotaFiscal.chave, chaves_solicitadas)

    produtos_solicitados = nf.select(pl.col.cod_sku.unique()).collect().to_series().to_list()
    
    produtos = get_tabelas_solictações_por_chave(Produto, Produto.cod_sku, produtos_solicitados)
    
    nf = nf.join(produtos, on = 'cod_sku', how = 'left').with_columns([
        (pl.col.volume * pl.col.fator_hl).alias('volume'),
        (pl.col.volume_boni * pl.col.fator_hl).alias('volume_boni')
    ]).group_by([pl.col.chave]).agg([
        pl.col.volume.sum().alias('volume_tt'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('CERVEJA'))).sum().alias('volume_cerveja'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('NAB'))).sum().alias('volume_nab'),
        pl.col.volume.filter(pl.col.cod_sku.is_in(get_skus_cesta('MATCH'))).sum().alias('volume_match'),
        pl.col.volume_boni.sum(),
        pl.col.ttv_plan.sum(),
        pl.col.ttv_real.sum(),
    ]).with_columns([
        (pl.col.volume_boni / pl.col.volume_tt).alias('% Boni'),
        (1-(pl.col.ttv_real / pl.col.ttv_plan)).alias('% Desc'),
    ])

    base = base.join(
        cliente, on='chave', how='left'
    ).join(
        nf, on = 'chave', how='left'
        ).select([
            'Aprovar', 'data_inclusão', 'chave', 'geo', 'comercial_right', 'operação_right', 'cod_pdv', 'nome_fantasia',
            'segmento', 'NGE', 'segmentação_primária', 'volume_cerveja', 'volume_nab', 'volume_match', '% Boni', '% Desc', 'print_aprovação', 'id'
        ]).with_columns(pl.col(x)*100 for x in ['% Desc', '% Boni']).collect()

    with st.form(key = 'Relatório de Aprovação', border=False):
        st.data_editor(base.to_pandas(), height=base.shape[0]*35+40, hide_index=True, key='editor_segmento', column_config = {
            'id': st.column_config.NumberColumn(disabled=True),
            'Aprovar': st.column_config.CheckboxColumn(),
            'print_aprovação': st.column_config.ImageColumn('Print do OK'),
            'segmentação_primária': 'Seg Primária',
            'data_inclusão': st.column_config.DateColumn('Data', format='DD/MM/YYYY'),
            'volume_cerveja': st.column_config.NumberColumn('Vol Cerveja', default='float', format='%.2f'),
            'volume_nab': st.column_config.NumberColumn('Vol NAB', default='float', format='%.2f'),
            'volume_match': st.column_config.NumberColumn('Vol Match', default='float', format='%.2f'),
            'cod_pdv': 'PDV',
            'segmento': 'Seg Solicitado',
            'nome_fantasia': 'Nome Fantasia',
            'operação_right': 'Operação',
            'comercial_right': 'Comercial',
            '% Desc': st.column_config.NumberColumn('% Desconto Total', default='float', format='%.2f %%'),
            '% Boni': st.column_config.NumberColumn('% Bonificação', default='float', format='%.2f %%')
        })
        st.form_submit_button('Salvar', type='primary')