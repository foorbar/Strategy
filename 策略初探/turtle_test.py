import numpy as np
import pandas as pd


class TurtleStrategy(object):
    def __init__(self, data_df, init_cash):
        self.sloop = 15
        # 默认本金
        self.init_cash = init_cash
        # 持有的本金
        self.cash = init_cash
        # 原始数据
        self.df = data_df
        # 计算N值的天数
        self.number_days = 20
        # N值列表
        self.N = []
        # 交易天数
        self.days = 0
        # 允许承受的最大损失
        self.loss = 0.1
        # 超过最大损失时的调整率
        self.adjust = 0.8
        # 交易使用的金额占总金额比例
        self.ratio = 0.8
        # 初始单元
        self.unit = 4
        # 最大允许单元
        self.unit_limit = 4
        # 突破价格
        self.break_price = 0
        # 持有的仓数
        self.position = 0
        # 收盘价的平均值
        self.mean = np.mean(self.df['close'])
        # 方向 1:buy,-1:sell
        self.buy_sell = 0

        self.capital_market_value = []
        self.res_df = pd.DataFrame()

    def handle_data(self):
        for i in range(0, len(self.df['close_time'])):
            current_price = self.df['close'][i]
            self.days = i
            self.N = self.df['N'].tolist()[:i+1]
            if self.days > self.number_days:
                value = self.get_value(i)
                if self.position == 0:
                    # if value < (1 - self.loss) * self.cash:
                    #     self.cash *= self.adjust
                    #     value *= self.adjust
                    self.market_in(current_price, self.ratio*self.cash, self.days)
                else:
                    self.market_add(current_price, self.ratio * self.cash, self.days)
                    self.market_out(current_price, self.days)
                    self.stop_loss(current_price, self.days)
                self.capital_market_value.append(self.get_value(i))
        self.res_df['capital_market_value'] = pd.Series(self.capital_market_value)
        self.res_df['daily_return_diff'] = round(self.res_df['capital_market_value']/self.res_df['capital_market_value'].shift(1)-1, 4)
        self.res_df['benchmark'] = self.df['close']
        self.res_df['date'] = self.df['close_time']
        # self.res_df.to_csv('./turtle.csv')

    def market_in(self, current_price, cash, date):
        # print(cash)
        if current_price < self.df['min'][date]:
            self.buy_sell = -1
            number_able = int(cash/current_price)
            self.unit = round(cash  * 0.01 / self.N[-1],2)
            # print("Uint short", self.unit)
            # print('number_able', number_able)
            if number_able >= self.unit:
                if abs(self.position) < self.unit_limit*self.unit:
                    print(self.df['close_time'][date] + ':')
                    print(' 进场 short')
                    current_price += self.buy_sell * self.sloop
                    self.break_price = current_price
                    print('current_price', current_price)
                    print(self.N[-1])
                    commission = self.set_commission(current_price, self.unit)
                    self.cash -= commission
                    self.cash -= current_price * self.unit * self.buy_sell
                    self.position += self.unit
                    print('self.cash:', self.cash)
                    print(' 目前的仓位为：{}'.format(self.position))
        elif current_price > self.df['max'][date]:
            self.buy_sell = 1
            number_able = int(cash/current_price)
            self.unit = round(cash  * 0.01 / self.N[-1],2)
            # print("Uint long ",self.unit)
            # print('number_able', number_able)
            if number_able >= self.unit:
                if abs(self.position) < self.unit_limit*self.unit:
                    print(self.df['close_time'][date] + ':')
                    print(' 进场 long')
                    current_price += self.buy_sell * self.sloop
                    self.break_price = current_price
                    print('current_price', current_price)
                    print(self.N[-1])
                    commission = self.set_commission(current_price, self.unit)
                    self.cash -= commission
                    self.cash -= current_price * self.unit * self.buy_sell
                    self.position += self.unit
                    print('self.cash:', self.cash)
                    print(' 目前的仓位为：{}'.format(self.position))

    def stop_loss(self, current_price, date):
        if self.buy_sell == -1 and current_price > (self.break_price + 2*(self.N[-1])) and self.position != 0:
            current_price -= self.sloop * self.buy_sell
            print('current_price', current_price)
            print(self.df['close_time'][date] + ':')
            print(' 止损 short')
            commission = self.set_commission(current_price, self.position)
            self.cash -= commission
            self.cash += current_price * self.position * self.buy_sell
            self.position = 0
            print(' ' + str(self.cash))
        elif self.buy_sell == 1 and current_price < (self.break_price - 2*(self.N[-1])) and self.position != 0:
            current_price -= self.sloop * self.buy_sell
            print(current_price)
            print(self.df['close_time'][date] + ':')
            print(' 止损 long')
            commission = self.set_commission(current_price, self.position)
            self.cash -= commission
            self.cash += current_price * self.position * self.buy_sell
            self.position = 0
            print(' ' + str(self.cash))

    def market_out(self, current_price, date):
        if self.buy_sell == -1 and current_price > self.df['max_10'][date] and self.position != 0:
            print(self.df['close_time'][date] + ':')
            print(' 离场 short')
            current_price -= self.sloop * self.buy_sell
            print(current_price)
            commission = self.set_commission(current_price, self.position)
            print("commission:",commission)
            print("self.cash:", self.cash)
            self.cash -= commission
            self.cash += current_price * self.position * self.buy_sell
            self.position = 0
            print(' ' + str(self.cash))
        elif self.buy_sell == 1 and current_price < self.df['min_10'][date] and self.position != 0:
            print(self.df['close_time'][date] + ':')
            print(' 离场 long')
            current_price -= self.sloop * self.buy_sell
            print(current_price)
            commission = self.set_commission(current_price, self.position)
            self.cash -= commission
            self.cash += current_price * self.position * self.buy_sell
            self.position = 0
            print(' ' + str(self.cash))

    def market_add(self, current_price, cash, date):
        if self.buy_sell == -1 and current_price <= self.break_price - 1*(self.N[-1]) and self.position != 0:
            current_price -= self.sloop * self.buy_sell
            number_able = cash/current_price
            if number_able >= self.unit:
                if abs(self.position) < self.unit_limit*self.unit:
                    print(self.df['close_time'][date] + ':')
                    print(' 加仓 short')
                    print(current_price)
                    commission = self.set_commission(current_price, self.unit)
                    self.cash -= commission
                    self.cash -= current_price * self.unit * self.buy_sell
                    print("add ",self.cash)
                    self.position += self.unit
                    self.break_price = current_price
                    value = self.get_value(date)
                    print(' ' + str(value))
                    print(' 目前的仓位为：{}'.format(self.position))
        elif self.buy_sell == 1 and current_price >= self.break_price + 1*(self.N[-1]) and self.position != 0:
            current_price -= self.sloop * self.buy_sell
            number_able = cash/current_price
            if number_able >= self.unit:
                if abs(self.position) < self.unit_limit*self.unit:
                    print(self.df['close_time'][date] + ':')
                    print(' 加仓 long')
                    print(current_price)
                    commission = self.set_commission(current_price, self.unit)
                    self.cash -= commission
                    self.cash -= current_price * self.unit * self.buy_sell
                    self.position += self.unit
                    self.break_price = current_price
                    value = self.get_value(date)
                    print(self.cash)
                    print(' ' + str(value))
                    print(' 目前的仓位为：{}'.format(self.position))

    def set_commission(self, price, amount):
        commussion = price * amount * 0.003
        if commussion > 10:
            return commussion
        else:
            return 10

    def get_value(self, i):
        value = self.cash + self.df['close'][i] * self.position * self.buy_sell
        return value

