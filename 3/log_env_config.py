from sqlalchemy import create_engine
from datetime import datetime


'''Подключение к БД, путь к папкам'''
ENGINE_STR = "postgresql+psycopg2://neoflex_user:neoflex_user@localhost:5432/neoflex_first"
TMP_PATH_SAVE_FILES = '/home/grigorii/docs/neoflex/dags/etl_first/3/src'
ENGINE = create_engine(ENGINE_STR)


def log_to_table(message):
    insert_statement = query = f'''
        INSERT INTO logs.to_csv_logs
        VALUES (
            '{datetime.now()}',
            '{message}' 
        );
    '''
    ENGINE.execute(insert_statement)
