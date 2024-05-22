import streamlit as st
import polars as pl

from ui_functions import lista_comerciais, lista_operações, lista_tabelas, tipo_solicitações, make_aprovação_tabelas_especiais, make_aprovação_segmento
from models import Solicitação_Segmento, Solicitação_Tabela_Especial, Session, engine, select


st.set_page_config(
    page_title="Aprovações", page_icon="📈", layout='wide', initial_sidebar_state='collapsed'
)

ss = st.session_state

st.title('Aprovações')

col1, col2, col3 = st.columns(3)

with col1:
    st.selectbox('Tipo de solicitação', tipo_solicitações, key='selected_solicitações')

with Session(engine) as session:
    if ss['selected_solicitações'] == 'Ajuste de Segmento':
        statement = select(Solicitação_Segmento.operação).distinct()
    if ss['selected_solicitações'] == 'Inclusão/Exclusão PDV de Tabela Especial':
        statement = select(Solicitação_Tabela_Especial.operação).distinct()
    list_op = list(session.exec(statement).fetchall())

with col2:
    st.multiselect('Operações', list_op, default=list_op, key='selected_operações')

with Session(engine) as session:
    if ss['selected_solicitações'] == 'Ajuste de Segmento':
        solicitante = select(Solicitação_Segmento.nome_usuario).where(Solicitação_Segmento.operação.in_(ss['selected_operações'])).distinct()
    if ss['selected_solicitações'] == 'Inclusão/Exclusão PDV de Tabela Especial':
        solicitante = select(Solicitação_Tabela_Especial.nome_usuario).where(Solicitação_Tabela_Especial.operação.in_(ss['selected_operações'])).distinct()

    list_solicit = list(session.exec(solicitante).fetchall())

with col3:
    st.multiselect('Solicitante', list_solicit, default=list_solicit, key='selected_solicitante')

try:
    match ss['selected_solicitações']:
        case 'Ajuste de Segmento':
            make_aprovação_segmento(ss['selected_operações'], ss['selected_solicitante'])
        case 'Inclusão/Exclusão PDV de Tabela Especial':
            make_aprovação_tabelas_especiais(ss['selected_operações'], ss['selected_solicitante'])
except pl.ColumnNotFoundError:
    st.write('sem ações selecionadas')

        

