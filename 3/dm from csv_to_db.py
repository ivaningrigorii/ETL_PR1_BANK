from sqlalchemy import MetaData, Table, delete
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from log_env_config import ENGINE, \
    TMP_PATH_SAVE_FILES, log_to_table


'''Загрузка данных обратно в postgres'''
def return_to_postgres(df, file):
    log_to_table(f'start -> from csv to db {file}')

    metadata_obj = MetaData(schema = 'dm')
    table = Table(file, metadata_obj, autoload_with=ENGINE)

    ENGINE.execute(delete(table))
    insert_statement = insert(table).values(df.values.tolist())
    ENGINE.execute(insert_statement)
    log_to_table(f'end -> from csv to db {file}')


'''Обработка данных из таблицы turnover'''
def turnover_to_postgres():
    file = 'dm_account_turnover_f'
    
    df = pd.read_csv(
        filepath_or_buffer=f'{TMP_PATH_SAVE_FILES}/{file}.csv', 
        header='infer',
        parse_dates=['on_date', ]
    ).fillna(0)

    return_to_postgres(df, file)



'''Обработка данных из таблицы 101 формы'''
def f101_to_postgres():
    file = 'dm_f101_round_f'

    df = pd.read_csv(
        filepath_or_buffer=f'{TMP_PATH_SAVE_FILES}/{file}.csv', 
        header='infer',
        parse_dates=['from_date', 'to_date']
    ).fillna(0)

    return_to_postgres(df, file)


def main():
    turnover_to_postgres()
    f101_to_postgres()


if __name__ == '__main__':
    main()
