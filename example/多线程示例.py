#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.append(root_path)
from trade_mdforopenctp import PeopleQuantApi
import time as tm
import zhuchannel
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
TradeFrontAddr = envs["7x24"]["td"]
MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

#策略函数
def cta(symbol_ctp ):
    quote1 = pqapi.get_quote(symbol_ctp)        #获取合约行情
    position1 = pqapi.get_position(symbol_ctp)   #获取合约持仓
    UpdateTime = quote1.ctp_datetime #行情更新时间
    lot = 1 #下单手数
    while True:
        if UpdateTime != quote1.ctp_datetime: #新行情推送
            UpdateTime = quote1.ctp_datetime
            buy_up = 1  #多头信号
            sell_down = 1  #空头信号
            if buy_up and not position1.pos_long:
                price = quote1["AskPrice1"]
                r = pqapi.open_close(symbol_ctp,"kaiduo",lot,price)
                print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position1.open_price_long,position1.open_price_short)
            elif sell_down and not position1.pos_short:
                price = quote1["BidPrice1"]
                r = pqapi.open_close(symbol_ctp,"kaikong",lot,price)
                print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position1.open_price_long,position1.open_price_short)
            if position1.pos_long and abs(quote1.LastPrice - position1.open_price_long) >= 2:
                price = quote1["BidPrice1"]
                r = pqapi.open_close(symbol_ctp,"pingduo",position1.pos_long,price)
                print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position1.open_price_long,position1.open_price_short)
            if position1.pos_short and abs(quote1.LastPrice - position1.open_price_short) >= 2:
                price = quote1["AskPrice1"]
                r = pqapi.open_close(symbol_ctp,"pingkong",position1.pos_short,price)
                print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position1.open_price_long,position1.open_price_short)
                    
  
#创建api实例
pqapi = PeopleQuantApi()
account = pqapi.get_account()            #获取账户资金
#创建策略线程
cta1 = zhuchannel.WorkThread(cta,args=('fu2601', ),kwargs={})
cta2 = zhuchannel.WorkThread(cta,args=('hc2601', ),kwargs={})
cta1.start()
cta2.start()
cta1.join
cta2.join