
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import abc
from collections import Counter
import math
import const
from .sql_utils import get_connection, get_engine


def parse_name(name):
    parts = name.split('.')
    return const.Security(parts[0]), parts[1], const.Source(parts[2])


def infer_frequency(data: pd.DataFrame):
    times = data.time.values
    freq = Counter(((times[1:] - times[: -1]) / np.timedelta64(1, 'ms')).round()).most_common()[0]
    if freq[1] / (data.shape[0] - 1) < 0.95:
        return const.Freq.ZERO
    return const.Freq(math.ceil(freq[0] / 60000))


def split_dataframe(data: pd.DataFrame, step):
    step = int(step)
    pointer = 0
    result = []
    while pointer < data.shape[0]:
        result.append(data.iloc[pointer: pointer + step])
        pointer += step
    return result


class DataSource(metaclass=abc.ABCMeta):
    # class attributes, to establish connection only once
    engine = None
    con = None

    def __init__(self, name, schema=None, new=False):
        if DataSource.engine is None:
            DataSource.engine = get_engine()
            DataSource.con = DataSource.engine.connect()
            self.initialize()

        self.name = name
        self.table_name = self.name.replace(".", "_").lower()
        self.schema = schema
        self.fields = list(schema.keys())
        self.sec_type, self.ticker, self.source = parse_name(name)

        # build queries
        fields = ', '.join(f'{k} {v}' for k, v in self.schema.items())
        self.table_creation_query = f'CREATE TABLE IF NOT EXISTS {self.table_name} ( {fields} )'

        fields = ('%s, ' * len(self.fields))[: -2]
        self.insert_query = f'INSERT INTO {self.table_name} VALUES ({fields})'

        # find object in database
        res = self.read(f'SELECT last_update, freq FROM data_source_master WHERE name = "{name}"')
        if not res:
            if new:
                DataSource.con.execute(f'INSERT INTO data_source_master VALUES ("{name}", NULL, NULL)')
                DataSource.con.execute(self.table_creation_query)
                self.last_update, self.freq = const.MinDate, None
            else:
                raise ValueError(f'data source {name} doesn''t exists')
        else:
            self.last_update, self.freq = res[0]

        if self.last_update is None:
            self.last_update = const.MinDate

        # update if not up-to-date
        latest = datetime.now() - timedelta(1)  # type: datetime
        if self.last_update < latest:
            data = self.load_new_data(latest)  # type: pd.DataFrame
            freq = infer_frequency(data)

            if self.freq is not None and freq > self.freq:
                raise RuntimeError(f'Frequency mismatch. {freq.name} (new) vs {self.freq.name} (original)')

            last_update = data.time.max()
            print(f'{name} updated to {last_update} with frequency {freq.name}')

            blocks = split_dataframe(data, 1E4)
            for idx, block in enumerate(blocks):
                print(f'\r{idx / len(blocks) * 100:.2f}%', end='')
                block.to_sql(self.table_name, con=DataSource.engine, if_exists='append', index=False)

            DataSource.con.execute(f'''
                UPDATE data_source_master 
                SET last_update = "{last_update}", freq = {freq.value} 
                WHERE name = "{name}"
            ''')

    def initialize(self):
        with self.engine.begin() as con:
            con.execute('''
                CREATE TABLE IF NOT EXISTS data_source_master (
                    name VARCHAR(20) NOT NULL PRIMARY KEY,
                    freq INT,
                    last_update DATETIME
                )
            ''')

    @staticmethod
    def read(sql):
        return DataSource.con.execute(sql).fetchall()

    def insert(self, sql, data=None):
        if data is not None:
            self.cur.executemany(sql, data)
        else:
            self.cur.execute(sql)
        self.con.commit()

    @abc.abstractmethod
    def load_new_data(self, latest):
        """
        return data that is ready to input
        """
        pass


class BarDataSource(DataSource):
    def __init__(self, name, schema, new):
        super(BarDataSource, self).__init__(name, schema, new)
        self._panel = pd.DataFrame(columns=['open', 'high', 'low', 'close'], dtype=float)

    @property
    def open(self):
        return self._panel.open

    @property
    def high(self):
        return self._panel.high

    @property
    def low(self):
        return self._panel.low

    @property
    def close(self):
        return self._panel.close

    @property
    def panel(self):
        return self._panel

    @abc.abstractmethod
    def load_new_data(self, latest):
        pass


class TiingoSource(BarDataSource):
    def __init__(self, name, new=False):
        sec_type, _, _ = parse_name(name)
        if sec_type != const.Security.EQUITY:
            raise ValueError('Tiingo only supports equity')

        schema = {}
        super(TiingoSource, self).__init__(name, schema, new=new)

    def load_new_data(self, latest):
        pass


