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

#策略函数
def cta(symbol_ctp ):
    quote1 = pqapi.get_quote(symbol_ctp)        #获取合约行情
    position1 = pqapi.get_position(symbol_ctp)   #获取合约持仓
    task = zhuchannel.WorkThread(pqapi.auto_pos_thread,args=(symbol_ctp,),kwargs={"return_queue":None})
    task.start()
    UpdateTime = quote1.ctp_datetime #行情更新时间
    while True:
        if UpdateTime != quote1.ctp_datetime: #新行情推送
            UpdateTime = quote1.ctp_datetime
            buy_up = 1  #多头信号
            sell_down = 1  #空头信号
            if buy_up :
                pqapi.set_pos_volume(symbol_ctp,pos_long=15)
            elif sell_down :
                pqapi.set_pos_volume(symbol_ctp,pos_short=15)
            if position1.pos_long and abs(quote1.LastPrice - position1.open_price_long) >= 2:
                pqapi.set_pos_volume(symbol_ctp,pos_long=0)
            if position1.pos_short and abs(quote1.LastPrice - position1.open_price_short) >= 2:
                pqapi.set_pos_volume(symbol_ctp,pos_short=0)
                    
#创建api实例
pqapi = PeopleQuantApi()
account = pqapi.get_account()            #获取账户资金
#创建策略线程
cta2 = zhuchannel.WorkThread(cta,args=('hc2601', ),kwargs={})
cta2.start()
