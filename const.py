
from enum import Enum, IntEnum
import numpy as np

ZorroPath = 'C:\\Users\\Albert\\Resilio Sync\\FXBootcamp\\Daily'
# ZorroPath = 'C:\\Users\\Albert\\Resilio Sync\\FXBootcamp\\.sync\\Archive\\Daily'
MinDate = np.datetime64('1990-01-01')


class Security(Enum):
    EQUITY = 'EQ'
    FX = 'FX'


class Source(Enum):
    Tiingo = 'T'
    ZORRO = 'Z'
    BAR = 'D'


class Freq(IntEnum):
    ZERO = 0    # no frequency
    M1 = 1      # 1 minute
    M5 = 5      # 5 minute
    M30 = 30    # half hour
    H1 = 60     # an hour
    H4 = 240    # 4 hours
    D = 1440    # daily
    W = 10080   # weekly
    M = 302400  # monthly
    Q = 907200  # quarterly
    Y = 3628800     # annual


Phases = {
    Freq.M5: {'valid': [0], 'unit': 0, 'denom': '5m'},
    Freq.H1: {'valid': [0], 'unit': 0, 'denom': 'h'},
    Freq.D: {'valid': list(range(24)), 'unit': np.timedelta64(1, 'h'), 'denom': 'D'}
}


sec_table_map = {
    Security.EQUITY: 'master_eq',
    Security.FX: 'master_fx'
}
