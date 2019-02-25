
import numpy as np
from .feature import Feature
from .schedule import Always


class Strategy:
    def __init__(self, name, features=None, algo=None, schedule=None):
        self.name = name
        self.features = features  # type: List[Feature]
        self.recursive_features = []
        self.discrete_features = []
        self.algo = algo
        self.schedule = schedule
        self.lookback = 0
        self.epoch = 0
        self.input = {}
        self.tickers = None

    def setup(self, tickers):
        self.tickers = tickers

        # setup algo
        self.algo.setup(tickers, self.features)

        # setup schedule
        if self.schedule is None:
            self.schedule = Always()

        # setup regular data
        self.input['time'] = np.empty(0, dtype='datetime64[ns]')
        self.input['close'] = np.empty((0, len(tickers)))

        # setup feature data
        if self.features is not None:
            self.lookback = max([x.lookback for x in self.features])
            for feature in self.features:
                self.input[feature.name] = feature

                if feature.is_recursive:
                    self.recursive_features.append(feature)
                else:
                    self.discrete_features.append(feature)

        return self

    def __call__(self, datum):
        """
        Returns relative weight

        :param datum:
        :return:
        """
        # update regular data
        self.input['time'] = np.append(self.input['time'], datum[0])
        self.input['close'] = self.append_data(self.input['close'], datum[1]['close'])

        # only recursive needed updating every bar
        for feature in self.recursive_features:
            feature(self.input)

        # calculate weight
        weights = None  # None weight means no change
        if self.epoch > self.lookback and self.schedule(datum[0]):
            # update features
            for feature in self.discrete_features:
                feature(self.input)
            weights = self.algo(self.input)  # input has references to features
        self.epoch += 1
        return weights

    def append_data(self, data, datum):
        return np.append(data[-self.lookback:], [datum], axis=0)
