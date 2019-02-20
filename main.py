
import at
import const

data = at.get(['AUDUSD', 'GBPUSD', 'EURUSD'], const.Freq.D, '2010-01-01', '2018-12-31')
print(data.head())
print(data.tail())

