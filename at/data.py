
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from collections import Counter
import math
from pathlib import Path
from struct import Struct

import const


def parse_name(name):
    parts = name.split('.')
    return const.Security(parts[0]), parts[1], const.Source(parts[2])


def get_table_name(name):
    return name.replace('.', '_').lower()


def infer_frequency(data: pd.DataFrame):
    times = data.time.values
    freq = Counter(((times[1:] - times[: -1]) / np.timedelta64(1, 'ms')).round()).most_common()[0]
    if freq[1] / (data.shape[0] - 1) < 0.95:
        return const.Freq.ZERO
    return const.Freq(math.ceil(freq[0] / 60000))


def convert_time_to_value(time):
    return pd.to_timedelta(time - const.MinDate).total_seconds() * 137438953472 / 86400 + 4674750981739642880


def convert_to_time(values):
    values = (1440 * (values - 4674750981739642880) / 137438953472).round()
    return np.datetime64('1990-01-01') + values * np.timedelta64(1, 'm')


def load_zorro_data(ticker, start_time, end_time):
    start_time, end_time = pd.to_datetime([start_time, end_time])

    data = []
    last_update_value = convert_time_to_value(start_time)

    for year in range(start_time.year, end_time.year + 1):
        file = Path(const.zorro_path) / f'{ticker}_{year}.t6'
        if not file.exists():
            if start_time == const.MinDate:
                continue
            print(f'year {year} is missing')
            break

        with open(file, 'rb') as f:
            pattern = Struct('qffffff')  # zorro t6 format
            for row in pattern.iter_unpack(f.read()):
                if row[0] <= last_update_value:
                    break
                data.append(row)

    final = pd.DataFrame(data, columns=['time', 'high', 'low', 'open', 'close', 'adjusted', 'volume']) \
        .sort_values(by='time')
    final.time = convert_to_time(final.time)
    final = final[final.time > start_time]
    return final.drop_duplicates('time', keep='last')


def load_tiingo_data(name, start_time, end_time):
    pass


zorro_schema = '''
    CREATE TABLE IF NOT EXISTS {} (
        time    DATETIME NOT NULL PRIMARY KEY,
        high    DOUBLE,
        low     DOUBLE,
        open    DOUBLE,
        close   DOUBLE,
        adjusted DOUBLE,
        volume  DOUBLE
    )
'''


tiingo_schema = '''
    CREATE TABLE IF NOT EXISTS {} (
        time    DATETIME NOT NULL PRIMARY KEY,
        high    DOUBLE,
        low     DOUBLE,
        open    DOUBLE,
        close   DOUBLE,
        adjusted DOUBLE,
        volume  DOUBLE
    )
'''


bar_schema = '''
    CREATE TABLE IF NOT EXISTS {} (
        time    DATETIME NOT NULL PRIMARY KEY,
        high    DOUBLE,
        low     DOUBLE,
        open    DOUBLE,
        close   DOUBLE
    )
'''


source_map = {
    const.Source.ZORRO: {'price_loader': load_zorro_data, 'schema': zorro_schema},
    const.Source.Tiingo: {'price_loader': load_tiingo_data, 'schema': tiingo_schema},
    const.Source.BAR: {'schema': bar_schema}
}


def update_data(name, engine, start_time, end_time, ori_freq=None):
    _, ticker, source = parse_name(name)  # this can also check name

    if ori_freq is None:
        print(f'[{name}] try to create new time series')
    else:
        print(f'[{name}] update time series')

    # load and validate data
    print(f'[{name}] loading data ... ', end='')
    data = source_map[source]['price_loader'](ticker, start_time, end_time)  # type: pd.DataFrame
    print('done')

    if data.empty:
        print(f'[{name}] series up-to-date')
        return
    new_freq = infer_frequency(data)

    if new_freq is const.Freq.ZERO:
        raise RuntimeError(f'[{name}] unrecognized frequency')

    if ori_freq is not None and new_freq > ori_freq:
        raise RuntimeError(f'[{name}] new frequency {new_freq.name} is lower than original {ori_freq.name}')

    # save result
    print(f'[{name}] {data.shape[0]} records with frequency {new_freq.name}')
    print(f'[{name}] saving to database ... ', end='')

    table_name = get_table_name(name)
    if ori_freq is None:  # new series
        with engine.begin() as con:
            con.execute(f'INSERT INTO tsdb VALUES("{name}", "{data.time.max()}", {new_freq.value})')
            con.execute(source_map[source]['schema'].format(table_name))
    data.to_sql(table_name, con=engine, if_exists='append', index=False, chunksize=10000)

    print('done')


def update_data_with_freq(name, engine, freq, phase, start_time, end_time, new=False):
    series_name = f'{name}.{freq.name}.{phase}'
    if new:
        print(f'[{series_name}] try to create new time series')
    else:
        print(f'[{series_name}] update time series')

    # check phase
    if phase not in const.Phases[freq]['valid']:
        raise ValueError(f'[{series_name}] phase {phase} is not allowed in freq {freq.name}')

    table_name = get_table_name(name)

    # load data
    print(f'[{series_name}] loading raw data from database ... ', end='')
    data = pd.read_sql(f'''
        SELECT * FROM {table_name}
        WHERE time > "{start_time}" AND  time <= "{end_time}"
    ''', con=engine).sort_values('time')
    print('done')

    # convert frequency
    phase_info = const.Phases[freq]
    data.time = (data.time - phase * phase_info['unit']).astype(f'datetime64[{phase_info["denom"]}]')
    data = data.groupby(by='time').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})

    # save result
    print(f'[{series_name}] {data.shape[0]} records with frequency {freq.name}')
    print(f'[{series_name}] saving to database ... ', end='')

    table_name = get_table_name(series_name)
    if new:  # new series
        with engine.begin() as con:
            con.execute(f'INSERT INTO tsdb VALUES("{series_name}", "{data.index.max()}", {freq.value})')
            con.execute(source_map[const.Source.BAR]['schema'].format(table_name))
    data.to_sql(table_name, con=engine, if_exists='append', index=True, chunksize=10000)

    print('done')


def get_tsdb_statuses(names, engine, freq):
    return pd.read_sql(f'''
        SELECT name, last_update, freq 
        FROM tsdb 
        WHERE name in ({', '.join([f'"{x}"' for x in names])})
    ''', con=engine).set_index('name').to_dict('index')


def get(names: list, freq, phase, start_time, end_time):
    engine = None
    try:
        engine = create_engine('mysql+mysqlconnector://user:12345678@localhost/secdb')
        start_time, end_time = pd.to_datetime([start_time, end_time])
        series_names = [f'{x}.{freq.name}.{phase}' for x in names]

        freq = const.Freq(freq)

        # initialize tsdb table if not exists
        with engine.begin() as con:
            con.execute('''
                CREATE TABLE IF NOT EXISTS tsdb (
                    name VARCHAR(20) NOT NULL PRIMARY KEY,
                    last_update DATETIME,
                    freq INT
                )
            ''')

        # update raw time series
        statuses = get_tsdb_statuses(names, engine, freq)
        for name in names:
            if name not in statuses:
                update_data(name, engine, const.MinDate, end_time)
            else:
                from_time = max(start_time, statuses[name]['last_update'])
                update_data(name, engine, from_time, end_time, const.Freq(statuses[name]['freq']))

        # check frequency
        statuses = get_tsdb_statuses(names + series_names, engine, freq)  # renew statuses
        for name, info in statuses.items():
            original_freq = const.Freq(info['freq'])
            if original_freq > freq:
                raise RuntimeError(f'[{name}] raw frequency {original_freq.name} is lower than required')

        # update freq specific time series
        for name, series_name in zip(names, series_names):
            if series_name not in statuses:
                update_data_with_freq(name, engine, freq, phase, const.MinDate, end_time, new=True)
            else:
                from_time = max(start_time, statuses[series_name]['last_update'])
                update_data_with_freq(name, engine, freq, phase, from_time, end_time, new=False)

        # load data and return
        panel = []
        for name in series_names:
            table_name = get_table_name(name)
            data = pd.read_sql(f'SELECT time, close FROM {table_name} WHERE time BETWEEN "{start_time}" AND "{end_time}"',
                               con=engine).set_index('time')
            panel.append(data)
        panel = pd.concat(panel, axis=1, keys=['.'.join(x.split('.')[: 2]) for x in names])\
            .reorder_levels([1, 0], axis=1).close
        return panel

    except RuntimeError as e:
        print(e)
