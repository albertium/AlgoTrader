
import at
import const

data = at.get(['FX.AUDUSD.Z', 'FX.GBPUSD.Z', 'FX.EURUSD.Z'], const.Freq.H1, 0, '2010-01-01', '2018-12-31')
print(data.head())