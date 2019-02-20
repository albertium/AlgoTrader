
import numpy as np
import pandas as pd
from collections import Counter
import math
from pathlib import Path
from struct import Struct

import const


# ================ Helpers ================
def infer_frequency(data: pd.DataFrame):
    times = data.time.values
    freq = Counter(((times[1:] - times[: -1]) / np.timedelta64(1, 'ms')).round()).most_common()[0]
    potential_freq = const.Freq(math.ceil(freq[0] / 60000))
    mode_pct = freq[1] / (data.shape[0] - 1)
    if potential_freq == const.Freq.D and mode_pct > 0.79:
        return const.Freq.D
    if mode_pct < 0.95:
        return potential_freq
    return const.Freq.ZERO


def convert_time_to_value(time):
    return pd.to_timedelta(time - const.MinDate).total_seconds() * 137438953472 / 86400 + 4674750981739642880


def convert_to_time(values):
    values = (1440 * (values - 4674750981739642880) / 137438953472).round()
    return np.datetime64('1990-01-01') + values * np.timedelta64(1, 'm')


# ================ Data Loaders ================
def load_zorro_data(ticker, start_time, end_time):
    start_time, end_time = pd.to_datetime([start_time, end_time])
    last_update_value = convert_time_to_value(start_time)

    # files to read
    root = Path(const.ZorroPath)
    if (root / f'{ticker}.t6').exists():
        files = [root / f'{ticker}.t6']
    else:
        files = [root / f'{ticker}_{year}.t6' for year in range(start_time.year, end_time.year + 1)]

    data = []
    for file in files:
        if not file.exists():
            print(f'{file} is missing')
            return None

        with open(file, 'rb') as f:
            pattern = Struct('qffffff')  # zorro t6 format
            for row in pattern.iter_unpack(f.read()):
                if row[0] <= last_update_value:
                    break
                data.append(row)

    final = pd.DataFrame(data, columns=['time', 'high', 'low', 'open', 'close', 'adjusted', 'volume']) \
        .sort_values(by='time').drop('adjusted', axis=1)
    final.time = convert_to_time(final.time)
    final = final[final.time >= start_time]
    return final.drop_duplicates('time', keep='last')


def load_tiingo_data(name, start_time, end_time):
    pass


# ================ Main API ================
def get(tickers: list, frequency, start_time, end_time, phase=0, source=const.Source.ZORRO):
    """
    only support reading Zorro data for now

    :param tickers: list of FX symbols like EURUSD
    :param freq:
    :param start_time:
    :param end_time:
    :param phase:
    :param source:
    :return:
    """

    if source == const.Source.ZORRO:
        data_loader = load_zorro_data
    else:
        raise ValueError(f'source {source.name} not implemented')

    print(f'[Data] reading {source.name} data from {const.ZorroPath}\n')

    start_time, end_time = pd.to_datetime([start_time, end_time])
    frequency = const.Freq(frequency)

    panel = []
    for ticker in tickers:
        # load data
        print(f'[{ticker}] loading data ... ', end='')
        data = data_loader(ticker, start_time, end_time)
        freq = infer_frequency(data)
        print(f'[{data.shape[0]} records at {freq.name}]')

        # check frequency
        if freq > frequency:
            raise RuntimeError(f'[{ticker}] raw frequency {freq.name} is lower than required')

        # convert frequency
        if freq < frequency:
            print(f'[{ticker}] converting {freq.name} to {frequency.name} ... ', end='')
            phase_info = const.Phases[frequency]
            data.time = (data.time - phase * phase_info['unit']).astype(f'datetime64[{phase_info["denom"]}]')
            data = data.groupby(by='time').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
            print(f'[{data.shape[0]} records]')

        panel.append(data.set_index('time'))
        print()

    # check data and return
    panel = pd.concat(panel, axis=1, keys=tickers).reorder_levels([1, 0], axis=1).sort_index(axis=1)
    missing_dates = panel[panel.isnull().any(axis=1)].index.tolist()

    print(f'[Data] {len(missing_dates)} rows have missing values')
    for idx in range(min(len(missing_dates), 10)):
        print(missing_dates[idx])
    print()

    return panel
