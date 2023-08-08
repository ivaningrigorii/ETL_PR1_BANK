import os

from sqlalchemy import create_engine, MetaData, Table, delete
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np
from datetime import datetime
from log_env_config import *


''' Загрузка данных в csv файл '''
def save_tmp(df, fname):
    os.makedirs(TMP_PATH_SAVE_FILES, exist_ok=True)
    df.fillna(0).to_csv(
        f'{TMP_PATH_SAVE_FILES}/{fname}.csv', 
        index=False, 
        encoding='UTF-8'
    )
    log_to_table(f'end -> from db to csv  {fname}')


''' Выгрузка данных из базы данных '''
def load_full_postgres_tables(table_name, date_cols):
    log_to_table(f'start -> from db to csv {table_name}')
    query = f'''
        SELECT *
        FROM dm.{table_name};
    '''

    df = pd.read_sql(
        sql=query, 
        con=ENGINE,
        parse_dates=date_cols
        )

    save_tmp(df, table_name)


def main():
    load_full_postgres_tables('dm_account_turnover_f', ['on_date',])
    load_full_postgres_tables('dm_f101_round_f', ['from_date', 'to_date'])

