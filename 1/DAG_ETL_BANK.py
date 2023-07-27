from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import numpy as np
from datetime import datetime

PATH_TO_FILES_CSV = '/home/grigorii/neoflex/project/src/1/1'

dag = DAG (
    'dag_etl_bank',
    start_date=days_ago(0, 0, 0, 0)
)

postgres_hook = PostgresHook(postgres_conn_id = 'postgres_neo_bank_1')
engine = create_engine(postgres_hook.get_uri())

# functions  --------------------------

''' Загрузка полученных df в БД '''
def load_to_db(df, table_name):
    metadata_obj = MetaData(schema = 'ds')
    table = Table(table_name, metadata_obj, autoload_with=engine)
    
    insert_statement = insert(table).values(df.values.tolist())
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=table.primary_key,
        set_=dict(insert_statement.excluded),
    )
    engine.execute(upsert_statement)


''' Сбор данных из ft_balance_f.csv, преобразование и загрузка в neo_bank_1  '''
def etl_ft_balance_f():
    df = pd.read_csv(filepath_or_buffer=f'{PATH_TO_FILES_CSV}/ft_balance_f.csv', 
        header = 'infer', 
        sep = ';',
        usecols = range(1,5),
        parse_dates = ['ON_DATE', ],
        dayfirst = True
    )
    
    load_to_db(df, 'ft_balance_f')


''' Сбор данных из et_md_account_d.csv, преобразование и загрузка в neo_bank_1  '''
def etl_md_account_d():
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/md_account_d.csv', 
        header='infer', 
        sep=';',
        usecols=range(1,8),
        parse_dates=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE']
    )
    load_to_db(df, 'md_account_d')


''' Сбор данных из ft_posting_f.csv, преобразование и загрузка в neo_bank_1  '''
def etl_ft_posting_f():
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/ft_posting_f.csv', 
        header='infer', 
        sep=';',
        parse_dates=['OPER_DATE', ],
        usecols=range(1,6)
    )
    
    df = df.drop_duplicates(
        subset=[
            'OPER_DATE', 
            'CREDIT_ACCOUNT_RK', 
            'DEBET_ACCOUNT_RK'
        ], keep='last'
    )
    
    load_to_db(df, 'ft_posting_f')


''' Сбор данных из md_currency_d.csv, преобразование и загрузка в neo_bank_1  '''
def etl_md_currency_d():
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/md_currency_d.csv', 
        header='infer', 
        sep=';',
        encoding = 'latin1',
        usecols=range(1,6),
        parse_dates=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'],
    )
    load_to_db(df, 'md_currency_d')


''' Сбор данных из md_currency_d.csv, преобразование и загрузка в neo_bank_1  '''
def etl_md_exchange_rate_d():
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/md_exchange_rate_d.csv', 
        header='infer', 
        sep=';',
        usecols=range(1,6),
        parse_dates=['DATA_ACTUAL_DATE', 'DATA_ACTUAL_END_DATE'],
    )
    df = df.drop_duplicates()
    
    load_to_db(df, 'md_exchange_rate_d')


''' Сбор данных из md_ledger_account_s.csv, преобразование и загрузка в neo_bank_1  '''
def etl_md_ledger_account_s():
    df = pd.read_csv(
        filepath_or_buffer=f'{PATH_TO_FILES_CSV}/md_ledger_account_s.csv', 
        header='infer', 
        sep=';',
        encoding = 'IBM866',
        parse_dates=['START_DATE', 'END_DATE'],
        usecols=range(1,29),
        dtype = {
            'PAIR_ACCOUNT': str,
            'MIN_TERM': str,
            'MAX_TERM': str,
            'MAX_TERM_MEASURE': str,
            'LEDGER_ACC_FULL_NAME_TRANSLIT': str,
            'IS_REVALUATION': str,
            'IS_CORRECT': str
        }
    )
    df = df.replace(np.nan, None)
    load_to_db(df, 'md_ledger_account_s')


def set_logs(**kwargs):
    columns = ['action_date', 'status']
    data = list(zip([datetime.now(), ], [kwargs['status_messange'], ]))

    pd.DataFrame(data=data, columns = columns) \
        .to_sql(
            name = 'logs_info_etl_11_process',
            con = engine,
            schema = 'logs',
            if_exists = 'append',
            index = False
        )

# operations -------------------------------

set_logs_start = PythonOperator(
    dag = dag,
    task_id = "set_logs_start",
    python_callable = set_logs,
    op_kwargs = {"status_messange":'START'},
)

sleep_5s = BashOperator(
    task_id="sleep_5s",
    dag=dag,
    bash_command="sleep 5s"
)

etl_ft_balance_f_ = PythonOperator(
    dag=dag, 
    task_id="ft_balance_f", 
    python_callable=etl_ft_balance_f
)

etl_md_account_d_ = PythonOperator(
    dag=dag, 
    task_id="md_account_d", 
    python_callable=etl_md_account_d
)

etl_ft_posting_f_ = PythonOperator(
    dag=dag, 
    task_id="ft_posting_f", 
    python_callable=etl_ft_posting_f
)

etl_md_currency_d_ = PythonOperator(
    dag=dag, 
    task_id="md_currency_d", 
    python_callable=etl_md_currency_d
)

etl_md_exchange_rate_d_ = PythonOperator(
    dag=dag, 
    task_id="md_exchange_rate_d", 
    python_callable=etl_md_exchange_rate_d
)

etl_md_ledger_account_s_ = PythonOperator(
    dag=dag, 
    task_id="md_ledger_account_s", 
    python_callable=etl_md_ledger_account_s
)

set_logs_end = PythonOperator(
    dag = dag,
    task_id = "set_logs_end",
    python_callable = set_logs,
    op_kwargs = {"status_messange":'END'},
)

# start--------------------------------------

set_logs_start >> sleep_5s >> [
    etl_ft_balance_f_,
    etl_md_account_d_,
    etl_ft_posting_f_,
    etl_md_currency_d_,
    etl_md_exchange_rate_d_,
    etl_md_ledger_account_s_
] >> set_logs_end
