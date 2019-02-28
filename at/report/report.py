
import plotly.graph_objs as go
import numpy as np
import pandas as pd
from .html import HTMLReport


class Report:
    def __init__(self, equity):
        self.equity = equity

        # calculate metrics
        self.summary = {}
        self.calculate_cgar()
        self.calculate_sharpe()

    def show(self):
        report = HTMLReport(header='Performance Report')

        # populate performance table
        metrics = ['sharpe', 'cgar']
        df = pd.DataFrame([[x, self.summary[x]] for x in metrics])
        report.add_table(df, header='Summary')

        # populate charts
        chart = [go.Scatter(y=self.equity.equity, x=self.equity.index, name='equity')]
        report.add_chart(chart, 'Equity Curve')

        report.show()

    def calculate_cgar(self):
        equity = self.equity.equity
        value = np.power(equity[-1] / equity[0], 252 / equity.shape[0]) - 1
        self.summary['cgar'] = f'{value * 100: .2f}%'

    def calculate_sharpe(self):
        returns = self.equity.equity.pct_change()
        value = returns.mean() / returns.std() * np.sqrt(252)
        self.summary['sharpe'] = f'{value: .3f}'
