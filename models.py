from sqlmodel import Field, Session, SQLModel, create_engine, select, insert, update, delete, Column
from pprint import pprint
import streamlit as st
from datetime import datetime
from st_supabase_connection import SupabaseConnection 

import numpy as np
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)

def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)

def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)

SQLModel.__table_args__ = {'extend_existing': True}

engine = create_engine(st.secrets['SUPABASE_ANON'], echo=False)

class User(SQLModel, table=True):
    id: int | None = Field(primary_key=True)
    nome: str
    email: str = Field(default=None, nullable=True)
    cargo: str = Field(default='APR')

class Produto(SQLModel, table=True):
    cod_sku: int = Field(primary_key=True)
    nome_abrev: str
    nome_sku: str
    tipo_prod: str
    marketplace: str
    fator_hl: float
    marca: str
    embalagem: str

class Cliente(SQLModel, table=True):
    chave: str = Field(primary_key=True, nullable=False)
    geo: str | None
    unb: int
    cod_pdv: int
    comercial: str | None
    operação: str | None
    tipo_operação: str | None
    gv: int | None
    setor: int | None
    latitude: float | None
    longitude: float | None
    NGE: str | None
    segmentação_primária: str | None
    nome_fantasia: str | None
    documento: str | None
    status: str | None

class NotaFiscal(SQLModel, table=True):
    chave: str = Field(primary_key=True, nullable=False)
    unb: int
    cod_pdv: int
    cod_sku: int = Field(primary_key=True, nullable=False)
    volume: float
    volume_boni: float
    ttv_plan: float
    ttv_real: float
    disparsão: float
    percentual_desc: float

class RLS(SQLModel, table=True):
    role: str = Field(primary_key=True)
    autoriza: str = 'RLS'

class Cesta(SQLModel, table = True):
    id: int 
    cod_sku: int = Field(primary_key=True)
    nome_cesta: str
    nome_cesta_abrev: str

class Solicitação_Tabela_Especial(SQLModel, table = True):
    id: int | None = Field(default=None, primary_key=True)
    data_inclusão: datetime = Field(default_factory = datetime.now)
    nome_usuario: str
    email_usuario: str
    geo: str
    comercial: str
    operação: str
    cod_pdv: int
    tabela: str
    ação: str
    print_aprovação: str
    status: str = Field(default='Nova Solicitação')

class Solicitação_Segmento(SQLModel, table = True):
    id: int | None = Field(default=None, primary_key=True)
    data_inclusão: datetime | None = Field(default_factory = datetime.now)
    nome_usuario: str
    email_usuario: str
    geo: str
    comercial: str
    unb: str
    operação: str
    cod_pdv: int
    segmento: str
    print_aprovação: str
    status: str = Field(default='Nova Solicitação')

class Tabelas_Especiais(SQLModel, table = True):
    id: int | None = Field(default=None, primary_key=True)
    unb: int
    nome_tabela: str
    cod_sku: int
    desconto: float

if __name__ == '__main__':
    pass