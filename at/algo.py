
import abc
import numpy as np


class Algo(metaclass=abc.ABCMeta):
    def __init__(self, name, dependencies):
        self.name = name
        self.dependencies = dependencies

    def setup(self, tickers, features):
        # check dependencies
        features_fed = set([x.name for x in features])
        features_needed = set(self.dependencies)
        if not features_needed.issubset(features_fed):
            raise RuntimeError(f'[Algo {self.name}] missing features: {features_needed - features_fed}')

        # algo specific setup
        self.specific_setup(tickers)

    @abc.abstractmethod
    def specific_setup(self, tickers):
        pass

    @abc.abstractmethod
    def __call__(self, all_input):
        pass


class LongShort(Algo):
    def __init__(self, factor, N_long=3, N_short=3):
        super(LongShort, self).__init__('longshort', [factor])
        self.factor = factor
        self.N_long = N_long  # number of asset to long
        self.N_short = N_short  # number of asset to short
        self.top = self.bottom = None

    def specific_setup(self, tickers):
        if self.N_long + self.N_short > len(tickers):
            raise ValueError(f'[Algo {self.name}] not enough assets to long short')
        self.top = len(tickers) - self.N_long - 1
        self.bottom = self.N_short
        self.orders = np.arange(len(tickers))

    def __call__(self, all_input):
        pos = np.argsort(all_input[self.factor].value)
        ranks = np.empty_like(pos)
        ranks[pos] = self.orders
        return (ranks > self.top).astype('float') - (ranks < self.bottom).astype('float')
