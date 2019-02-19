
import pandas as pd
import numpy as np
from pathlib import Path
import ctypes
from struct import Struct
from datetime import datetime, timedelta
import const
from .data_source import BarDataSource


class T6(ctypes.Structure):
    _fields_ = [
        ('time', ctypes.c_int64),
        ('high', ctypes.c_float),
        ('low', ctypes.c_float),
        ('open', ctypes.c_float),
        ('close', ctypes.c_float),
        ('val', ctypes.c_float),
        ('vol', ctypes.c_float)
    ]


def convert_to_time(values):
    values = (1440 * (values - 4674750981739642880) / 137438953472).round()
    return np.datetime64('1990-01-01') + values * np.timedelta64(1, 'm')


def convert_time_to_value(time):
    return (time - datetime(1990, 1, 1)).total_seconds() * 137438953472 / 86400 + 4674750981739642880


class ZorroSource(BarDataSource):
    def __init__(self, name, new=False):
        schema = {
            'time': 'DATETIME NOT NULL PRIMARY KEY',
            'high': 'DOUBLE',
            'low': 'DOUBLE',
            'open': 'DOUBLE',
            'close': 'DOUBLE',
            'adjusted': 'DOUBLE',
            'volume': 'DOUBLE'
        }

        super(ZorroSource, self).__init__(name, schema, new=new)

    def load_new_data(self, latest):
        ticker = self.ticker.replace('/', '')

        data = []
        last_update_value = convert_time_to_value(self.last_update)

        for year in range(self.last_update.year, latest.year + 1):
            file = Path(const.zorro_path) / f'{ticker}_{year}.t6'
            if not file.exists():
                if self.last_update == const.MinDate:
                    continue
                print(f'year {year} is missing')
                break

            with open(file, 'rb') as f:
                pattern = Struct('qffffff')  # zorro t6 format
                for row in pattern.iter_unpack(f.read()):
                    if row[0] <= last_update_value:
                        break
                    data.append(row)

        final = pd.DataFrame(data, columns=['time', 'high', 'low', 'open', 'close', 'adjusted', 'volume'])\
            .sort_values(by='time')
        final.time = convert_to_time(final.time)
        final = final[final.time > self.last_update]
        return final.drop_duplicates('time', keep='last')
