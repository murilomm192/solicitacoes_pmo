import streamlit as st
from ui_functions import make_forms, salvar_ações, depara_operação_unb

st.set_page_config(
    page_title="Solicitações", page_icon="💸", layout='wide', initial_sidebar_state='collapsed'
)

ss = st.session_state

st.title('Solicitações de Preços')

if 'n_ação' not in ss:
    ss['n_ação'] = 1

def increment():
    ss['n_ação'] = ss['n_ação'] + 1

def send():
    salvar_ações(ss)
    st.success('enviado')
    st.session_state.clear()

col1, col2 = st.columns([1,2])
    
with col1:
    st.text_input('Nome:', key='nome_solicitante')
with col2:
    st.text_input('Email:', key='email_solicitante')

for x in range(ss['n_ação']):
    make_forms(ss, x)

st.button('Nova Ação', on_click=increment, type='secondary')

st.button('Enviar Ações', on_click=send, type='primary')




    