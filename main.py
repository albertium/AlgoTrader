
import at
import const
from matplotlib import pyplot as plt


data = at.get(['AUDUSD', 'GBPUSD', 'EURUSD'], const.Freq.D, '2018-09-01', '2018-12-31')
strategy = at.Strategy('momo',
                       features=[at.feature.RateOfChange(60)],
                       algo=at.algo.LongShort('roc', N_long=1, N_short=0))
backtest = at.BackTest(data, strategy)
res = backtest.run()
res.plot()
plt.show()
