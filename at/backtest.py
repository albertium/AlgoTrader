
import numpy as np
import pandas as pd
import const
from .report import Report


class BackTest:
    def __init__(self, data, strategy, broker='IB'):
        self.tickers = data.close.columns.values
        self.strategy = strategy.setup(self.tickers)
        self.data = data

        # broker information
        self.comm = const.BrokerProfiles[broker]['comm']

    def run(self):
        cash = 1E4
        shares = np.zeros(len(self.tickers))
        equity = []
        weights = None

        for epoch, datum in enumerate(self.data.iterrows()):
            closes = datum[1].close
            portfolio_value = cash + np.sum(shares * closes)
            if weights is not None:
                # re-balance
                new_shares = np.fix(portfolio_value * weights / closes)
                cash = portfolio_value - np.sum(np.abs(new_shares) * closes)  # assume shorting is 100% margin

                # calculate commission
                commission = np.sum(np.abs(new_shares - shares)) * self.comm
                cash -= commission
                portfolio_value -= commission

                shares = new_shares
                weights = None

            equity.append([datum[0], portfolio_value])

            # calculate new weights. None means no change
            weights = self.strategy(datum)
            if weights is not None:
                weights = weights / np.sum(np.abs(weights))

        equity = pd.DataFrame(equity, columns=['time', 'equity']).set_index('time')
        return Report(equity=equity)
