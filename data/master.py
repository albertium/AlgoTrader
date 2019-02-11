
from .zorro import ZorroSource
from .data_source import TiingoSource
import const


def get_security(name, new=False):
    parts = name.split('.')
    source = const.Source(parts[2])

    if source == const.Source.ZORRO:
        return ZorroSource(name, new)
    if source == const.Source.Tiingo:
        return TiingoSource(name, new)
