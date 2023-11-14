import pandas as pd
import numpy as np
import yfinance as yf
import itertools
import random
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio


# Define the list of stock symbols
symbols = ['AAPL', 'NVDA', 'LLY', '^GSPC', '^DJI',
           'MSFT', 'GOOGL', 'TSLA', 'JPM', 'AMZN', 
           'JNJ', 'WMT','PG','V', 'MA', 'BAC', 'CSCO',
           'INTC', 'VZ']
# Download historical data for the symbols
df = yf.download(symbols, start="2000-12-01", end="2022-12-01")

# Calculate daily returns
returns = np.log(df['Adj Close']).pct_change()
symbols = returns.columns

class PortfolioAnalyzer:
    def __init__(self):

        N = 255  # 255 trading days in a year
        rf = 0.01  # 1% risk-free rate
        self.returns = returns
        self.N = N
        self.rf = rf

    def sharpe_ratio(self, return_series):
        mean = return_series.mean() * self.N - self.rf
        sigma = return_series.std() * np.sqrt(self.N)
        return mean / sigma

    def sortino_ratio(self, series):
        mean = series.mean() * self.N - self.rf
        std_neg = series[series < 0].std() * np.sqrt(self.N)
        return mean / std_neg

    def max_drawdown(self, return_series):
        comp_ret = (return_series + 1).cumprod()
        peak = comp_ret.expanding(min_periods=1).max()
        dd = (comp_ret / peak) - 1
        return dd.min()

    def portfolioreturn(self, weights, samples):
        ret_func = np.dot(self.returns[samples].mean(), weights) * (255)
        return ret_func

    def portfoliovariance(self, weights, samples):
        por_var = np.dot(np.dot(self.returns[samples].cov(), weights), weights) * np.sqrt(255)
        return por_var

    def weightscreator(self, df):
        rand = np.random.random(df)
        rand /= rand.sum()
        return rand

    def efficientfrontier(self, num_assets, num_portfolios):

        global data, base_sharp, base_Maxdown, base_Sortino, selected_assets

        ret = []
        stds = []
        w = []
        sharpes = []
        max_downs = []
        sortinos = []
        w = []
        sorting = self.returns.columns

        select = sorting.to_frame().head(num_assets)  # Convert Index to DataFrame and select the top 'num_assets'
        selected_assets = list(select.index)

        for i in range(num_portfolios):
            weights = self.weightscreator(len(selected_assets))
            w.append(weights)
            sample = pd.DataFrame(self.returns[selected_assets].dot(weights))

            ret.append(self.portfolioreturn(weights, selected_assets))
            stds.append(self.portfoliovariance(weights, selected_assets))

            sharpes.append(np.mean(sample.apply(self.sharpe_ratio, axis=0)))
            max_downs.append(np.mean(sample.apply(self.max_drawdown, axis=0)))
            sortinos.append(np.mean(sample.apply(self.sortino_ratio,  axis=0)))
            
            data = pd.DataFrame(
                {'Return': ret, 'StdDev': stds, 'Sharpe': sharpes, 'MaxDrawdown': max_downs, 'Sortino': sortinos,
                 'Weights': w})
            base_sharp = data[data['Sharpe'] == data['Sharpe'].max()]
            base_Maxdown = data[data['MaxDrawdown'] == data['MaxDrawdown'].min()]
            base_Sortino = data[data['Sortino'] == data['Sortino'].max()]

        return data, base_sharp, base_Maxdown, base_Sortino

    def plotting(self, data, base_sharp, base_Maxdown, base_Sortino):
        fig = px.scatter(data, x='StdDev', y='Return', color='Sharpe', title='Portfolio Performance')
        fig.update_traces(marker=dict(size=10), selector=dict(mode='markers+text'))
        fig.update_layout(coloraxis_colorbar=dict(title='Sharpe Ratio'))

        fig.add_trace(
            go.Scatter(
                x=base_sharp['StdDev'],
                y=base_sharp['Return'],
                mode='markers',
                marker=dict(size=20, color='black', symbol='circle', opacity=1),
                name='Max Sharpe'
            )
        )

        fig.add_trace(
            go.Scatter(
                x=base_Maxdown['StdDev'],
                y=base_Maxdown['Return'],
                mode='markers',
                marker=dict(size=20, color='black', symbol='circle', opacity=1),
                name='Min Max Drawdown'
            )
        )

        fig.add_trace(
            go.Scatter(
                x=base_Sortino['StdDev'],
                y=base_Sortino['Return'],
                mode='markers',
                marker=dict(size=20, color='black', symbol='circle', opacity=1),
                name='Max Sortino'
            )
        )
        # This live of code is so improtant to display the plot inside the HTML code not opennign another tap, becasue by default plotly opens new tab!
        plot_div =pio.to_html(fig, full_html=False)
        return plot_div

    def printing_results(self, base_sharp, base_Maxdown, base_Sortino):

        weights_array_sharp = base_sharp['Weights'].iloc[0]
        weights_dict_sharp = {asset: weight for asset, weight in zip(selected_assets, weights_array_sharp)}

        weights_array_base_Maxdown = base_Maxdown['Weights'].iloc[0]
        weights_dict_base_Maxdown = {asset: weight for asset, weight in zip(selected_assets, weights_array_base_Maxdown)}

        weights_array_base_Sortino = base_Sortino['Weights'].iloc[0]
        weights_dict_base_Sortino = {asset: weight for asset, weight in zip(selected_assets, weights_array_base_Sortino)}

        shar = "Optimized Weights based on Sharp Ratio:" + str(weights_dict_sharp)
        mxdwn = "Optimized Weights based on Max Drawdown Ratio:" + str(weights_dict_base_Maxdown)
        srr = "Optimized Weights based on Sortino Ratio:" + str(weights_dict_base_Sortino)

        return shar, mxdwn, srr

