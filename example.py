#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from trade_mdforopenctp import PeopleQuantApi 
import time as tm
import zhuchannel
import os
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
TradeFrontAddr="tcp://180.168.146.187:10101"   #交易前置地址
MdFrontAddr="tcp://101.230.209.178:53313"      #行情前置地址
TradeFrontAddr = envs["电信1"]["td"]
MdFrontAddr = envs["电信1"]["md"]
#TradeFrontAddr = envs["7x24"]["td"]
#MdFrontAddr = envs["7x24"]["md"]

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
#建议先取行情后取持仓
symbol = "m2601"
quote1 = pqapi.get_quote(symbol)        #获取合约行情
position1 = pqapi.get_position(symbol)   #获取合约持仓
#position2 = pqapi.get_position("hc2601")
#quote2 = pqapi.get_quote("IC2512")

#print(quote1)
#margin1 = pqapi.get_symbol_marginrate("IH2509")
#margin2 = pqapi.get_symbol_marginrate("TF2512")  #获取合约保证金
#print(margin1,"IH2509")
#print(margin2,"TF2512")
#ls = pqapi.query_symbol_option("rb2601","PUT")
#op = pqapi.get_option("PUT",3160,0,ls[0])
#合约行情时间
local_timestamp = quote1.local_timestamp
#print(op)
#quote3 = pqapi.get_quote(op["option"])
#kline = pqapi.get_kline(op["option"], "5m", 5)   #获取K线
#tick = pqapi.get_tick(op["option"])                #获取Tick
#kline2 = pqapi.get_kline("rb2601", "10m", 5)   #获取K线
#tick2 = pqapi.get_tick("rb2601")                #获取Tick
print(quote1)
while True:
    #break
    if local_timestamp != quote1.local_timestamp: #新行情推送
        local_timestamp = quote1.local_timestamp
        print(('合约代码',quote1.InstrumentID,'最新价',quote1.LastPrice,"卖一价",quote1.AskPrice1,"买一价",quote1.BidPrice1,"成交量",quote1.Volume,quote1.ctp_datetime))
        #print(position1)
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
    #tm.sleep(5)