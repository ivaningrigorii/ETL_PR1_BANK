import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np
from datetime import datetime

PATH_TO_FILES_CSV = '/home/grigorii/neoflex/project/src/1/1'
TMP_PATH_SAVE_FILES = './dag_src'

postgres_hook = PostgresHook(postgres_conn_id = 'postgres_neo_bank_1')
engine = create_engine(postgres_hook.get_uri())


# загрузка и выгрузка pandas df в файл tmp файл csv --------
def save_tmp(df, fname):
    os.makedirs(TMP_PATH_SAVE_FILES, exist_ok=True)
    df.to_csv(
        f'{TMP_PATH_SAVE_FILES}/{fname}.csv', 
        index=False, 
        encoding='UTF-8'
    )

def read_tmp(fname):
    return pd.read_csv(
        filepath_or_buffer=f'{TMP_PATH_SAVE_FILES}/{fname}.csv', 
            header='infer',
            keep_default_na=False
    )


# ETL tasks  --------------------------
# -- extract tasks


''' Загрузка из csv, структура близка к табличной в БД'''
@task(task_id='extract_csv')
def extract_csv(name_file_csv, encode_type):
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/{name_file_csv}.csv', 
        header='infer', 
        sep=';',
        encoding = encode_type,
        keep_default_na=False
    )
    save_tmp(df, name_file_csv)



#-- transform tasks


@task(task_id='transform__md_ledger_account_s')
def transform_md_ledger_account_s(name_df):
    columns_type_date = ['START_DATE', 'END_DATE']

    pd_df = read_tmp(name_df)

    pd_df[columns_type_date] = pd_df[columns_type_date].apply(pd.to_datetime)
    pd_df = pd_df.iloc[: , 1:]

    save_tmp(pd_df, name_df)


@task(task_id='transform_md_currency_d')
def transform_md_currency_d(name_df):
    columns_type_date = ['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE']

    pd_df = read_tmp(name_df)
    
    pd_df[columns_type_date] = pd_df[columns_type_date].apply(pd.to_datetime)
    pd_df = pd_df.iloc[: , 1:]

    save_tmp(pd_df, name_df)


#-- load tasks

''' Загрузка полученных df в БД '''
@task(task_id='load_postgres')
def load_postgres(table_name):
    metadata_obj = MetaData(schema = 'ds')
    table = Table(table_name, metadata_obj, autoload_with=engine)
    
    df = read_tmp(table_name)
    insert_statement = insert(table).values(df.values.tolist())
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=table.primary_key,
        set_=dict(insert_statement.excluded),
    )
    engine.execute(upsert_statement)


# DAG -----------------------------------

'''Подбор тасков трансофрмации по имени файла'''
def select_transform_task(name_file):
    tasks_for_select = {
        "md_ledger_account_s": transform_md_ledger_account_s,
        "md_currency_d": transform_md_currency_d
    }

    return tasks_for_select[name_file]


with DAG("dag_etl_bank",
    start_date=datetime(2021, 1 ,1),
    schedule_interval='@daily', 
    catchup=False,
    description='Project 1.1',
    tags=['Neoflex', '1.1']
) as dag:

    sleep_5s = BashOperator(
        task_id="sleep_5s",
        dag=dag,
        bash_command="sleep 5s"
    )

    tasks_end_start = []
    for t_id in ['start', 'end']:
        @task(task_id=t_id)
        def set_logs():
            columns = ['action_date', 'status']
            data = list(zip([datetime.now(), ], t_id))

            pd.DataFrame(data=data, columns = columns) \
                .to_sql(name = 'logs_info_etl_11_process', con = engine, 
                    schema = 'logs', if_exists = 'append',index = False
                )
        tasks_end_start.append(set_logs())

    groups = []
    for g_id in ['md_ledger_account_s', 'md_currency_d']:
        tg_id = f"{g_id}_etl_group"

        @task_group(group_id=tg_id)
        def tg1():
            enc_tps = {
                'md_ledger_account_s': 'IBM866',
                'md_currency_d': 'CP866'
            }
            enc_t = enc_tps[g_id] if g_id in enc_tps else 'UTF-8'
            
            extract_csv(name_file_csv=g_id, encode_type=enc_t) >> \
            select_transform_task(g_id)(g_id) >> load_postgres(g_id)

        groups.append(tg1())


    tasks_end_start[0] >> sleep_5s >> [
        groups[0],
        groups[1]
    ] >> tasks_end_start[1]
    