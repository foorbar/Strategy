import pandas as pd
import numpy as np
import math
import talib
from pymongo import MongoClient
import time


class Strategy(object):

    def __init__(self, data_df, init_cash, shorttime, longtime):
        self.start_time = data_df.index.tolist()[0]
        self.end_time = data_df.index.tolist()[-1]

        Ssma = talib._ta_lib.SMA(data_df['close'], timeperiod=shorttime)
        Lsma = talib._ta_lib.SMA(data_df['close'], timeperiod=longtime)
        data_df['Ssma'] = Ssma
        data_df['Lsma'] = Lsma
        self.df = data_df

        self.init_cash = init_cash
        self.cash = init_cash
        self.position = 0

        d = list(date for date in range(1443196800000, 1544457600000, 86400000))
        self.date_range = d
        self.captial_market_value = []
        self.position_list = []
        self.res_df = pd.DataFrame()
        self.res_df['date'] = self.date_range

    def run_simlation(self):
        for date in self.date_range:
            sell_signal, sell_open_price = self.get_sell_signal(date)
            if sell_signal == 1:
                amount = self.get_sell_amount()
                if amount > 0:
                    commission = self.cal_cost_function(sell_open_price, amount)
                    self.cash += sell_open_price*amount
                    self.cash -= commission
                    self.position -= amount

            buy_signal, buy_open_price = self.get_buy_signal(date)
            if buy_signal == 1:
                amount = self.get_buy_amount(buy_open_price)
                if amount > 0:
                    commission = self.cal_cost_function(buy_open_price, amount)
                    self.cash -= buy_open_price*amount
                    self.cash -= commission
                    self.position += amount
            self.captial_market_value.append(self.get_market_value(date))
            self.position_list.append(self.position)
        self.res_df['captial_market_value'] = pd.Series(self.captial_market_value)
        self.res_df['daily_return_diff'] = round((self.res_df['captial_market_value']
                                                  /self.res_df['captial_market_value'].shift(1)-1), 4)
        self.res_df['position'] = self.position_list
        self.res_df['benchmark'] = self.get_benchmark_index()
        # self.res_df['benchmark'].fillna(method='bfill', inplace=True)
        # self.res_df['benchmark'].fillna(method='ffill', inplace=True)
        self.res_df.to_csv('./result.csv')

    def get_benchmark_index(self):
        df = self.df
        benchmark_list = []
        for date in self.date_range:
            if df[df['date'] == date].empty:
                benchmark_list.append(np.nan)
            else:
                benchmark_list.append(float(df[df['date'] == date]['close']))
        return benchmark_list

    # 获取当前持有的市�?
    def get_market_value(self, date):
        market_value = 0
        df = self.df
        if self.position != 0:
            close_price = df[df['date'] <= date].tail(1)['close']
            market_value += self.position * float(close_price)
        return round(market_value + self.cash, 2)

    # 获取卖出信号
    def get_sell_signal(self, date):
        df = self.df
        sell_signal = 0
        sell_open_price = 0

        if df[df['date'] == date].empty:
            return sell_signal, sell_open_price
        df = df[df['date'] <= date].tail(3)
        if len(df) == 3 and df.iloc[0]['Ssma'] > df.iloc[0]['Lsma'] and df.iloc[1]['Ssma'] < df.iloc[1]['Lsma']:
            sell_signal = 1
            sell_open_price = df.iloc[1]['open']
        return sell_signal, sell_open_price

    # 获取买入信号
    def get_buy_signal(self, date):
        df = self.df
        buy_signal = 0
        buy_open_price = 0

        if df[df['date'] == date].empty:
            return buy_signal, buy_open_price
        df = df[df['date'] <= date].tail(3)
        if len(df) == 3 and df.iloc[0]['Ssma'] < df.iloc[0]['Lsma'] and df.iloc[1]['Ssma'] > df.iloc[1]['Lsma']:
            buy_signal = 1
            buy_open_price = df.iloc[1]['open']
        return buy_signal, buy_open_price

    # 获取卖出的数�?
    def get_sell_amount(self):
        return self.position

    # 获取买入的数�?
    def get_buy_amount(self, price):
        if self.position == 0:
            amount = math.floor(self.init_cash / price)
            return amount
        else:
            return 0

    # 设置手续�?
    def cal_cost_function(self, price, amount):
        commission = price * amount * 0.0002
        if commission > 5:
            return commission
        else:
            return 5




if __name__ == '__main__':
    conn = MongoClient('0.0.0.0', 27017)
    db = conn['db_name']
    my_set = db['set_name']
    close_time = []
    close_price = []
    open_price = []
    datas = my_set.find()
    for data in datas:
        a_time = data['close_time']
        a_open = data['open_price']
        a_close = data['close_price']
        close_price.append(a_close)
        close_time.append(a_time)
        open_price.append(a_open)
    data_dict = { 'date': close_time, 'close': close_price, 'open': open_price}
    df = pd.DataFrame(data_dict)
    example = Strategy(df, 100000, 5, 10)
    example.run_simlation()


