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
from tqsdk import TqApi, TqKq,TqAuth, TqAccount, TqChan 
from tqsdk import TqSim, TqBacktest, BacktestFinished,TqTimeoutError

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
TradeFrontAddr = envs["电信2"]["td"]
MdFrontAddr = envs["电信2"]["md"]
TradeFrontAddr = envs["7x24"]["td"]
MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID=""   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

           
#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
account = pqapi.get_account()            #获取账户资金
loop = asyncio.SelectorEventLoop() #创建事件循环
asyncio.set_event_loop(loop)
api = TqApi( auth=TqAuth('',''),loop=loop, debug=False)
quote_major = api.get_quote('KQ.m@CZCE.SR')
instrument_id = quote_major.underlying_symbol.split('.')[1]
group_option_calls = pqapi.query_symbol_option(instrument_id,"CALL") #合约全部看涨期权
group_option_puts = pqapi.query_symbol_option(instrument_id,"PUT")  #合约全部看跌期权
t = tm.time()
op = {}
for group_option in zip(group_option_calls,group_option_puts):
    call_level0 = pqapi.get_option(quote_major.last_price,0,group_option[0])
    call_level1 = pqapi.get_option(quote_major.last_price,1,group_option[0])
    call_level_1 = pqapi.get_option(quote_major.last_price,-1,group_option[0])
    put_level0 = pqapi.get_option(quote_major.last_price,0,group_option[1])
    put_level1 = pqapi.get_option(quote_major.last_price,1,group_option[1])
    put_level_1 = pqapi.get_option(quote_major.last_price,-1,group_option[1])
    op.update({f"{group_option[0]['ExpireDate'].to_list()[0]}":[[call_level0,call_level1,call_level_1],[put_level0,put_level1,put_level_1]]})
print(f"用时:{tm.time()-t},标的合约价格:{quote_major.last_price},期权：{op}")
while True:
    api.wait_update()