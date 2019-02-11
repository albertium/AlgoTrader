
import mysql.connector as sql
from sqlalchemy import create_engine


def get_connection():
    return sql.connect(host='localhost', database='secdb', user='user', password='12345678')


def get_engine():
    return create_engine('mysql+mysqlconnector://user:12345678@localhost/secdb')


def check_table(cursor, table_name):
    res = cursor.execute(f'''
        select * from information_schema.tables where table_schema = "secdb" and table_name = {table_name}
    ''')
    return res is not None


def check_key_exists(cursor, table_name, keys: dict):
    where_clauses = []
    for k, v in keys.items():
        if isinstance(v, str):
            where_clauses.append(f'{k} = "{v}"')
        else:
            where_clauses.append(f'{k} = {v}')
    res = cursor.execute(f'select * from {table_name} where {" and ".join(where_clauses)}')
    return res is not None


def initialize_table_if_not_exists(cursor, table_name, name_type_map, primary_keys):
    query = f'''
        create table {table_name} if not exists (
            {','.join(n + ' ' + t for n, t in name_type_map.items())},
            primary key({','.join(primary_keys)})
        );
    '''
    cursor.execute(query)
    cursor.commit()


def initialize_secdb(cursor):
    # equity
    initialize_table_if_not_exists(cursor, 'master_eq', {
        'ticker': 'VARCHAR(10)',
        'source': 'VARCHAR(2)',
        'exchange': 'VARCHAR(2)',
        'timezone': 'TINYINT',
        'last_update': 'DATE'
    }, ['ticker', 'source'])

    initialize_table_if_not_exists(cursor, 'master_fx', {
        'ticker': 'VARCHAR(6)',
        'source': 'VARCHAR(2)',
        'timezone': 'TINYINT',
        'last_update': 'DATE'
    }, ['ticker', 'source'])
