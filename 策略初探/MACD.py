#!/usr/bin/env	python 
# _*_ coding:utf-8 _*_
import pandas as pd
import numpy as np
import math
import talib
from pymongo import MongoClient


class MACDstrategy(object):

    def __init__(self, data_df, init_cash, shorttime, longtime, timeperiod):
        self.start_time = data_df['date'].tolist()[0]
        self.end_time = data_df['date'].tolist()[-1]

        DIF, DEA, MACD = talib._ta_lib.MACD(data_df.close, fastperiod=shorttime, slowperiod=longtime, signalperiod=timeperiod)
        data_df['DIF'] = DIF
        data_df['DEA'] = DEA
        data_df['MACD'] = MACD
        self.df = data_df

        self.init_cash = init_cash
        self.cash = init_cash
        self.position = 0

        d = list(date for date in range(int(self.start_time), int(self.end_time), 86400000))
        self.date_range = d
        self.capital_market_value = []
        self.position_list = []
        self.res_df = pd.DataFrame()
        self.res_df['date'] = self.date_range

    def run_strategy(self):
        for date in self.date_range:
            sell_signal, sell_open_price = self.get_sell_signal(date)
            if sell_signal == 1:
                amount = self.get_sell_amount()
                if amount > 0:
                    commission = self.get_commission(sell_open_price, amount)
                    self.cash += sell_open_price*amount
                    self.cash -= commission
                    self.position -= amount

            buy_signal, buy_open_price = self.get_buy_signal(date)
            if buy_signal == 1:
                amount = self.get_buy_amount(buy_open_price)
                if amount > 0:
                    commission = self.get_commission(buy_open_price, amount)
                    self.cash -= buy_open_price*amount
                    self.cash -= commission
                    self.position += amount
            self.capital_market_value.append(self.get_market_value(date))
            self.position_list.append(self.position)
        self.res_df['capital_market_value'] = pd.Series(self.capital_market_value)
        self.res_df['daily_return_diff'] = round(self.res_df['capital_market_value'] /
                                                 self.res_df['capital_market_value'].shift(1)-1, 4)
        self.res_df['position'] = self.position_list
        self.res_df['benchmark'] = self.get_benchmark_index()
        self.res_df.to_csv('./macd.csv')

    def get_benchmark_index(self):
        df = self.df
        benchmark_list = []
        for date in self.date_range:
            if df[df['date'] == date].empty:
                benchmark_list.append(np.nan)
            else:
                benchmark_list.append(float(df[df['date'] == date]['close']))
        return benchmark_list

    def get_market_value(self, date):
        market_value = 0
        df = self.df
        if self.position != 0:
            close_price = df[df['date'] <= date].tail(1)['close']
            market_value += self.position*float(close_price)
        return round(market_value + self.cash, 2)

    def get_buy_signal(self, date):
        df = self.df
        buy_signal = 0
        buy_open_price = 0

        if df[df['date'] == date].empty:
            return buy_signal, buy_open_price
        df = df[df['date'] <= date].tail(3)
        if len(df) == 3 and 0 < df.iloc[0]['DIF'] < df.iloc[0]['DEA'] and df.iloc[1]['DIF'] > df.iloc[1]['DEA'] > 0:
            buy_signal =1
            buy_open_price = df.iloc[1]['open']
        return buy_signal, buy_open_price

    def get_sell_signal(self, date):
        df = self.df
        sell_signal = 0
        sell_open_price = 0
        if df[df['date'] == date].empty:
            return sell_signal, sell_open_price
        df = df[df['date'] <= date].tail(3)
        if len(df) == 3 and df.iloc[0]['DIF'] > df.iloc[0]['DEA'] > 0 and 0 < df.iloc[1]['DIF'] < df.iloc[1]['DEA']:
            sell_signal = 1
            sell_open_price = df.iloc[1]['open']
        return sell_signal, sell_open_price

    def get_buy_amount(self, price):
        if self.position == 0:
            amount = math.floor(self.init_cash/price)
            return amount
        else:
            return 0

    def get_sell_amount(self):
        return self.position

    def get_commission(self, price, amount):
        commission = price * amount * 0.0002
        if commission > 10:
            return commission
        else:
            return 10


if __name__ == '__main__':
    conn = MongoClient('192.168.1.97', 27017)
    db = conn['bitmex_kline']
    my_set = db['XBTUSD_1d']
    datas = my_set.find()
    close_time = []
    close_price = []
    open_price = []
    for data in datas:
        date = data['close_time']
        a_close = data['close_price']
        a_open = data['open_price']
        close_time.append(date)
        close_price.append(a_close)
        open_price.append(a_open)
    data_dict = {'date':close_time, 'close':close_price, 'open': open_price}
    df = pd.DataFrame(data_dict)
    example = MACDstrategy(df, 100000, 12, 26, 9)
    example.run_strategy()
