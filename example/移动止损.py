#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.insert(0,root_path)
from peoplequant.pqctp import PeopleQuantApi
import time as tm
from peoplequant import zhuchannel
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

#策略函数
def TrailingStop(symbol_ctp, stop_loss, take_profit, loss_step):
    '''
    日内操作,跨交易日需本地记录移动止损价
    stop_loss = 10 #默认止损跳数
    take_profit = 10 #初始盈利跳数,盈利该跳数后开启追踪止损
    loss_step = 10 #移动止损步长,止损价与最新价距离保持loss_step宽度
    '''
    quote = pqapi.get_quote(symbol_ctp)        #获取合约行情
    position = pqapi.get_position(symbol_ctp)   #获取合约持仓
    symbol_info = pqapi.get_symbol_info(symbol_ctp) #合约属性
    #多头、空头止损价
    loss_priceD, loss_priceK = float("nan"), float("nan")
    UpdateTime = quote.ctp_datetime #行情更新时间
    localtime = quote.local_timestamp
    lot = 1 #下单手数
    while True:
        if UpdateTime != quote.ctp_datetime and localtime != quote.local_timestamp: #新行情推送
            UpdateTime = quote.ctp_datetime
            localtime = quote.local_timestamp
            
            if position.pos_long : #有多单
                if quote.BidPrice1 < position.open_price_long + take_profit * symbol_info.PriceTick or loss_priceD != loss_priceD: #未到初始盈利
                    loss_priceD = position.open_price_long - stop_loss * symbol_info.PriceTick  #初始止损价
                else : #盈利超过初始盈利跳数
                    if quote.BidPrice1 > loss_priceD + loss_step * symbol_info.PriceTick: #修改止损价
                        loss_priceD = quote.BidPrice1 - loss_step * symbol_info.PriceTick
                if quote.BidPrice1 < loss_priceD: #价格跌到止损价
                    r = pqapi.open_close(symbol_ctp,"pingduo",position.pos_long,quote.BidPrice1)
            if position.pos_short : #有空单
                if quote.AskPrice1 > position.open_price_short - take_profit * symbol_info.PriceTick or loss_priceK != loss_priceK: #未到初始盈利
                    loss_priceK = position.open_price_short + stop_loss * symbol_info.PriceTick  #初始止损价
                else : #盈利超过初始盈利跳数
                    if quote.AskPrice1 < loss_priceK - loss_step * symbol_info.PriceTick: #修改止损价
                        loss_priceK = quote.AskPrice1 + loss_step * symbol_info.PriceTick
                if quote.AskPrice1 > loss_priceK: #价格涨到止损价
                    r = pqapi.open_close(symbol_ctp,"pingkong",position.pos_short,quote.AskPrice1)
  
#创建api实例
pqapi = PeopleQuantApi()
account = pqapi.get_account()            #获取账户资金
#创建策略线程
cta1 = zhuchannel.WorkThread(TrailingStop,args=('fu2601',10,10,10 ),kwargs={})
cta2 = zhuchannel.WorkThread(TrailingStop,args=('hc2601',10,10,10 ),kwargs={})
cta1.start()
cta2.start()
cta1.join
cta2.join