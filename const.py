
from enum import Enum, IntEnum
from datetime import datetime

zorro_path = 'C:\\Users\\Albert\\Resilio Sync\\History'
MinDate = datetime(1990, 1, 1)


class Security(Enum):
    EQUITY = 'EQ'
    FX = 'FX'


class Source(Enum):
    Tiingo = 'T'
    ZORRO = 'Z'


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


sec_table_map = {
    Security.EQUITY: 'master_eq',
    Security.FX: 'master_fx'
}
