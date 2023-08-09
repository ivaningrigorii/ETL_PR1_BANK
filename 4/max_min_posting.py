import os
from sqlalchemy import create_engine
import pandas as pd


ENGINE_STR = "postgresql+psycopg2://neoflex_user:neoflex_user@localhost:5432/neoflex_first"
TMP_PATH_SAVE_FILES = '/home/grigorii/docs/neoflex/dags/etl_first/4/res'
ENGINE = create_engine(ENGINE_STR)
clear = lambda: os.system('clear')


''' Вызов функции min_max_posting в postgres '''
def min_max_posting_func(date):
    df = pd.read_sql(
        sql='SELECT * FROM ds.min_max_posting(%(date)s)',
        params={"date":date,},
        con=ENGINE
    )
    return df


''' Печать в консоль результата (для красоты) '''
def print_result(df):
    clear()

    date_from_df = df['date_posting'].values.tolist()[0]
    print(f"Дата: {date_from_df}")

    pd.set_option('display.float_format', str)
    print('\nРезультат: ')
    print(df[df.columns[1:]].to_string(index=False))


''' Сохранение в CSV файл '''
def save_to_csv(df):
    os.makedirs(TMP_PATH_SAVE_FILES, exist_ok=True)
    df.to_csv(
        f'{TMP_PATH_SAVE_FILES}/max_min_posting.csv',
        index=False
    )
    print('\nСохранено в CSV файл.')


def main():
    clear()
    date = str(input('Введите дату >>> '))

    df = min_max_posting_func(date)
    print_result(df)
    save_to_csv(df)


if __name__ == '__main__':
    main()

