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
    },
    "电信1": {
        "td": "tcp://182.254.243.31:30001",
        "md": "tcp://182.254.243.31:30011",
    },
    "电信2": {
        "td": "tcp://182.254.243.31:30002",
        "md": "tcp://182.254.243.31:30012",
    },
    "移动": {
        "td": "tcp://182.254.243.31:30003",
        "md": "tcp://182.254.243.31:30013",
    },
}
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

#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
account = pqapi.get_account()            #获取账户资金
#建议先取行情后取持仓
quote1 = pqapi.get_quote("FG601")        #获取合约行情
position1 = pqapi.get_position("FG601")   #获取合约持仓
#position2 = pqapi.get_position("hc2601")
#quote2 = pqapi.get_quote("rb2601")

#print(quote1)
#margin1 = pqapi.get_symbol_marginrate("IH2509")
#margin2 = pqapi.get_symbol_marginrate("TF2512")  #获取合约保证金
#print(margin1,"IH2509")
#print(margin2,"TF2512")
#ls = pqapi.query_symbol_option("rb2601","PUT")
#op = pqapi.get_option("PUT",3160,0,ls[0])
#合约行情时间
UpdateTime = quote1.ctp_datetime
#print(op)
#quote3 = pqapi.get_quote(op["option"])
#kline = pqapi.get_kline(op["option"], "5m", 5)   #获取K线
#tick = pqapi.get_tick(op["option"])                #获取Tick
#kline2 = pqapi.get_kline("rb2601", "10m", 5)   #获取K线
#tick2 = pqapi.get_tick("rb2601")                #获取Tick
while True:
    #break
    if UpdateTime != quote1.ctp_datetime: #新行情推送
        UpdateTime = quote1.ctp_datetime
        print(quote1["InstrumentID"],quote1["LastPrice"],quote1["ctp_datetime"],)
        print(position1)
        #print(quote2["InstrumentID"],quote2["LastPrice"],quote2["UpdateTime"],)
        #print(position2 )
        print(account)
        #print(quote3)
        #print(margin2)
        #print(kline.data)
        #print(kline.data.select(["InstrumentID", "period_start",   "open", "high","low", "close", "Volume","OpenInterest","period" ]))
        #print(tick.data.select(["InstrumentID","TradingDay","LastPrice","BidPrice1","AskPrice1","BidVolume1","AskVolume1","ctp_datetime"]).tail(5))
        #print(kline2.data.select(["InstrumentID", "period_start",   "open", "high","low", "close", "Volume","OpenInterest","period" ]))
        #print(tick2.data.select(["InstrumentID","TradingDay","LastPrice","BidPrice1","AskPrice1","BidVolume1","AskVolume1","ctp_datetime"]).tail(5))
    tm.sleep(5)