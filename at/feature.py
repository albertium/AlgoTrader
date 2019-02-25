
import abc


class Feature(metaclass=abc.ABCMeta):
    def __init__(self, name, lookback, recursive):
        self.name = name
        self.lookback = lookback
        self.is_recursive = recursive

    @abc.abstractmethod
    def __call__(self, base_input):
        pass


class RateOfChange(Feature):
    def __init__(self, lookback):
        super(RateOfChange, self).__init__('roc', lookback + 1, False)
        self.value = None

    def __call__(self, base_input):
        closes = base_input['close']
        self.value = closes[-1] / closes[-self.lookback - 1] - 1
