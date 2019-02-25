
import abc


class Schedule(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @abc.abstractmethod
    def __call__(self, time):
        pass


class Always(Schedule):
    def __call__(self, time):
        return True
