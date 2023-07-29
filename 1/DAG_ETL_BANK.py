import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.decorators import task, task_group

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np
from datetime import datetime


PATH_TO_FILES_CSV = '/home/grigorii/neoflex/project/src/1/1'
TMP_PATH_SAVE_FILES = './dag_src'
FILES_CSV = [
    'ft_balance_f',
    'md_account_d',
    'md_currency_d',
    'ft_posting_f',
    'md_exchange_rate_d',
    'md_ledger_account_s'
]

postgres_hook = PostgresHook(postgres_conn_id = 'postgres_neo_bank_1')
ENGINE = create_engine(postgres_hook.get_uri())


# Загрузка и выгрузка pandas df в файл tmp файл csv --------
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
#--


# -- EXTRACT

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



#-- TRANSFORM

@task(task_id='transform__ft_balance_f')
def transform_ft_balance_f(name_df):
    pd_df = read_tmp(name_df)
    pd_df['ON_DATE'] = pd.to_datetime(
        pd_df['ON_DATE'], dayfirst=True)
    pd_df = pd_df.iloc[: , 1:]
    save_tmp(pd_df, name_df)


@task(task_id='transform_del_1_col')
def transform_del_1_col(name_df):
    pd_df = read_tmp(name_df)
    pd_df = pd_df.iloc[: , 1:]
    save_tmp(pd_df, name_df)


@task(task_id='transform_posting')
def transform_posting(name_df):
    pd_df = read_tmp(name_df)

    pd_df = pd_df.iloc[: , 1:]
    pd_df = pd_df.groupby([
        'OPER_DATE', 
        'CREDIT_ACCOUNT_RK', 
        'DEBET_ACCOUNT_RK'
    ], as_index=False).sum()
    
    save_tmp(pd_df, name_df)


@task(task_id='transform_rm_dpls')
def transform_rm_dpls(name_df):
    pd_df = read_tmp(name_df)
    pd_df = pd_df.iloc[: , 1:]
    pd_df = pd_df.drop_duplicates()
    save_tmp(pd_df, name_df)


@task(task_id='transform_datetime')
def transform_datetime(name_df):
    pd_df = read_tmp(name_df)
    pd_df = pd_df.iloc[: , 1:]

    pd_df['DATA_ACTUAL_DATE'] \
        = pd.to_datetime(pd_df['DATA_ACTUAL_DATE'])
    pd_df['DATA_ACTUAL_END_DATE'] \
        = pd.to_datetime(pd_df['DATA_ACTUAL_END_DATE'])

    save_tmp(pd_df, name_df)
    

#-- LOAD

''' Загрузка полученных df в БД '''
@task(task_id='load_postgres')
def load_postgres(table_name):
    metadata_obj = MetaData(schema = 'ds')
    table = Table(table_name, metadata_obj, autoload_with=ENGINE)
    
    df = read_tmp(table_name)
    insert_statement = insert(table).values(df.values.tolist())
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=table.primary_key,
        set_=dict(insert_statement.excluded),
    )
    ENGINE.execute(upsert_statement)


'''Подбор тасков трансофрмации по имени файла'''
def select_transform_task(name_file):
    tasks_for_select = {
        "ft_balance_f": transform_ft_balance_f,
        "md_account_d": transform_datetime,
        "md_currency_d": transform_del_1_col,
        "ft_posting_f": transform_posting,
        "md_exchange_rate_d": transform_rm_dpls,
        "md_ledger_account_s": transform_del_1_col,
    }

    return tasks_for_select[name_file]


#-- DAG 

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

    # Логи для всего процесса --

    def query_process_log(time_log, type_log):
            return f'''
                INSERT INTO logs.logs_info_etl_11_process 
                    (action_date, status)
                VALUES ('{time_log}', '{type_log}'
            )'''

    
    @task(task_id='start')
    def start(**kwargs):
        ti = kwargs['ti']
        time_start_process = datetime.now()
        ENGINE.execute(query_process_log(time_start_process, 'start'))
        pk_log = ENGINE.execute(f'''
            SELECT
                l.log_id
            FROM logs.logs_info_etl_11_process as l
            ORDER BY l.log_id DESC
            LIMIT 1 
        ''').fetchone()[0]
        ti.xcom_push(value=pk_log, key='tstart')

    @task(task_id='end')
    def end():
        ENGINE.execute(query_process_log(datetime.now(), 'end'))



    groups = []
    for g_id in FILES_CSV:
        tg_id = f"{g_id}_etl_group"
        log_id = f'{g_id[:6]}_log'

        def query_task_log(type_log, ti, tname):
            pk_log = ti.xcom_pull(task_ids='start', key='tstart')
            return f'''
                INSERT INTO logs.log_table 
                VALUES ('{tname}', '{datetime.now()}',
                '{type_log}', {pk_log})
            '''

        # Логи для отдельных ETL процессов --
        @task(task_id=f't_start')
        def t_log_start(table_for_query, **kwargs):
            ENGINE.execute(query_task_log('start', kwargs['ti'], table_for_query))

        @task(task_id=f't_end')
        def t_log_end(table_for_query, **kwargs):
            ENGINE.execute(query_task_log('end', kwargs['ti'], table_for_query))
        #--


        ''' Группы ETL '''
        @task_group(group_id=tg_id)
        def tg1():
            enc_tps = {
                'md_ledger_account_s': 'IBM866',
                'md_currency_d': 'CP866'
            }
            enc_t = enc_tps[g_id] if g_id in enc_tps else 'UTF-8'
            
            extract_csv(name_file_csv=g_id, encode_type=enc_t) >> \
            select_transform_task(g_id)(g_id) >> load_postgres(g_id)

        ''' Группы ETL с логированием '''
        @task_group(group_id=log_id)
        def log_group_exe(**kwargs):
            t_log_start(g_id) >> tg1() >> t_log_end(g_id)

        groups.append(log_group_exe())


    start() >> sleep_5s >> groups >> end()
    