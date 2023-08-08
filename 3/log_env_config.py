from sqlalchemy import create_engine
from datetime import datetime


'''Подключение к БД, путь к папкам'''
ENGINE = create_engine(
    "postgresql+psycopg2://neoflex_user:neoflex_user@localhost:5432/neoflex_first"
)
TMP_PATH_SAVE_FILES = './src'

def log_to_table(message):
    insert_statement = query = f'''
        INSERT INTO logs.to_csv_logs
        VALUES (
            '{datetime.now()}',
            '{message}' 
        );
    '''
    ENGINE.execute(insert_statement)
