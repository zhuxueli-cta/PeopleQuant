#!/usr/bin/env python
#  -*- coding: utf-8 -*-
'''
作为天勤量化的辅助指令检查工具,减少天勤程序的错误指令发出的概率
'''
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.insert(0,root_path)
from peoplequant.pqctp import PeopleQuantApi
from tqsdk import TqApi, TqKq,TqAuth, TqAccount, TqChan 
from tqsdk import TqSim, TqBacktest, BacktestFinished,TqTimeoutError
from tqsdk.ta import MA
import time as tm
import asyncio
import traceback
import types
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any

envs = {
    "7x24": {
        "td": "tcp://182.254.243.31:40001",
        "md": "tcp://182.254.243.31:40011",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信1": {
        "td": "tcp://182.254.243.31:30001",
        "md": "tcp://182.254.243.31:30011",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信2": {
        "td": "tcp://182.254.243.31:30002",
        "md": "tcp://182.254.243.31:30012",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "移动": {
        "td": "tcp://182.254.243.31:30003",
        "md": "tcp://182.254.243.31:30013",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
}
TradeFrontAddr="tcp://180.168.146.187:10101"   #交易前置地址
MdFrontAddr="tcp://101.230.209.178:53313"      #行情前置地址
TradeFrontAddr = envs["电信1"]["td"]
MdFrontAddr = envs["电信1"]["md"]
#TradeFrontAddr = envs["7x24"]["td"]
#MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

#策略主体
async def cta(symbol,**kw ):
    #loop = asyncio.get_running_loop()
    symbol_ctp = symbol.split('.')[-1] #ctp合约代码
    #获取合约CTP行情
    quote1 = await loop.create_task(pqapi.async_wrapper(pqapi.get_quote,symbol_ctp ))
    symbol_info = pqapi.get_symbol_info(symbol_ctp) #合约属性
    position1 = pqapi.get_position(symbol_ctp)   #获取合约持仓
    kline = await api.get_kline_serial(symbol,3600)  #获取天勤K线
    quote = await api.get_quote(symbol)  #获取天勤行情
    lot = 1 #下单手数
    balance = kw["balance"] if "balance" in kw else 0  #账户最低权益
    risk_ratio = kw["risk_ratio"] if "risk_ratio" in kw else 1  #账户风险度
    #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、重复报单数   阈值
    orders_insert,orders_cancel,daylots,self_trade,order_exe,repeat = 500,500,5000,10,4000,2000
    async with api.register_update_notify(quote) as update_chan:
        async for _ in update_chan:
            #查询合约全部活动委托单
            orders = pqapi.get_symbol_order(InstrumentID=symbol_ctp,OrderStatus='Alive',_print=False)
            tasks = []
            for order in orders:
                task = loop.create_task(pqapi.async_wrapper(pqapi.check_order,order ))
                tasks.append(task)
            if tasks: #等待活动委托单结束
                 rs = await asyncio.gather(*tasks)
                 for r in rs:
                    if r['shoushu']: pass
                    elif r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
            account = pqapi.get_account()
            #权益足够,风险度足够,否则只平不开
            risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
            #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
            orderrisk = pqapi.get_order_risk(symbol_ctp)
            order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and
                            orderrisk["self_trade_count"] < self_trade and orderrisk["order_exe"] < order_exe
                             )
            ma = MA(kline,10) #计算10日均线
            buy_up = risk_control and quote.last_price > ma.iloc[-1].ma > ma.iloc[-2].ma and quote.last_price > kline.iloc[-1].close  #多头信号
            sell_down = risk_control and quote.last_price < ma.iloc[-1].ma < ma.iloc[-2].ma and quote.last_price < kline.iloc[-1].close  #空头信号
            buy_loss = position1.open_price_long - (quote1.AskPrice1 - symbol_info.PriceTick) >= 2*symbol_info.PriceTick  #多单止损
            buy_profit = (quote1.AskPrice1 - symbol_info.PriceTick) - position1.open_price_long >= 2*symbol_info.PriceTick  #多单止盈
            sell_profit = position1.open_price_short - (quote1.BidPrice1 + symbol_info.PriceTick) >= 2*symbol_info.PriceTick  #空单止盈
            sell_loss = (quote1.BidPrice1 + symbol_info.PriceTick) - position1.open_price_short >= 2*symbol_info.PriceTick  #空单止损
            if order_enable:
                if buy_up and not position1.pos_long:
                    price = "超价"
                    r = await loop.create_task(pqapi.OpenClose(symbol_ctp,"kaiduo",lot,price))
                    if r['shoushu']: pass
                    elif r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                elif sell_down and not position1.pos_short:
                    price = "超价"
                    r = await loop.create_task(pqapi.OpenClose(symbol_ctp,"kaikong",lot,price))
                    if r['shoushu']: pass
                    elif r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                if position1.pos_long and (buy_loss or buy_profit):
                    price = "超价"
                    r = await loop.create_task(pqapi.OpenClose(symbol_ctp,"pingduo",lot,price))
                    if r['shoushu']: pass
                    elif r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                if position1.pos_short and (sell_loss or sell_profit):
                    price = "超价"
                    r = await loop.create_task(pqapi.OpenClose(symbol_ctp,"pingkong",lot,price))
                    if r['shoushu']: pass
                    elif r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
            else: #权益、风险度、报撤单等超过阈值
                if orderrisk["order_count"] >= orders_insert: print(f"{symbol_ctp},退出策略执行警告，报单笔数已超预警{orderrisk['order_count']} >= {orders_insert}")
                if orderrisk["cancel_count"] >= orders_cancel: print(f"{symbol_ctp},退出策略执行警告，撤单笔数已超预警{orderrisk['cancel_count']} >= {orders_cancel}")
                if orderrisk["order_exe"] >= order_exe: print(f"{symbol_ctp},退出策略执行警告，信号量已超预警{orderrisk['order_exe']} >= {order_exe}")   
                return
            if not risk_control: print(f"{symbol_ctp},退出策略执行警告，权益:{account.Balance}、风险度:{account.risk_ratio}已超预警")
#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
loop = asyncio.SelectorEventLoop() #创建事件循环
asyncio.set_event_loop(loop)
api = TqApi(account=TqAccount(broker_id='',account_id='',password=''), auth=TqAuth(user_name='',password='')
    ,loop=loop, debug=False,
            )
account = pqapi.get_account()            #获取账户资金
#创建策略协程任务
cta_task1 = loop.create_task(cta(symbol='SHFE.fu2601'))
cta_task2 = loop.create_task(cta(symbol='SHFE.hc2601'))

while True:
    api.wait_update()