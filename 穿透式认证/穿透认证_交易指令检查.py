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

TradeFrontAddr=" "   #交易前置地址
MdFrontAddr=" "      #行情前置地址
BROKERID=""   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="client_pqapi_1.0"   #客户端ID
AUTHCODE=""  #授权码

'''接口适应性'''
#创建api实例,连接交易前置,登录账户
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, 
                       TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="",ProductionMode=False)
account = pqapi.get_account()  #获取账户资金
symbol = 'rb2601'
quote = pqapi.get_quote(symbol)  #获取合约行情
symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
position = pqapi.get_position(symbol)        #获取合约持仓

'''错误防范,交易指令检查'''
r = pqapi.open_close('rb26995',"kaiduo",1,quote["AskPrice1"],order_info='验证合约码错误') #错误合约码
print(f"验证合约码错误,错误信息:{r['last_msg']}")
r = pqapi.open_close(symbol,"kaiduo",0.1,quote["AskPrice1"],order_info='验证下单数量错误') #错误数量（非整数)
print(f"验证下单数量错误,错误信息:{r['last_msg']}")
r = pqapi.open_close(symbol,"kaiduo",1,quote["AskPrice1"]+0.5,order_info='验证下单价格错误') #错误价格(非最小跳整数倍)
print(f"验证下单价格错误,错误信息:{r['last_msg']}")
r = pqapi.open_close(symbol,"pingduo",1000,quote["BidPrice1"],order_info='验证平仓数量不足') #平仓手数不足,验仓
print(f"验证平仓数量不足,错误信息:{r['last_msg']}")
r = pqapi.open_close(symbol,"kaiduo",0,quote["BidPrice1"],order_info='验证单笔最小下单数量') #低于单笔最小下单数量
print(f"验证单笔最小下单数量,错误信息:{r['last_msg']}")
r = pqapi.open_close(symbol,"kaiduo",100000000000,quote["UpperLimitPrice"],order_info='验证单笔最大下单数量') #超出单笔最大下单数量
print(f"验证单笔最大下单数量,下单数量:{100000000000},超出最大委托数量截取为最大委托数量:{r['shoushu']}")
#验证可用平仓数量不足,验仓
r = pqapi.open_close(symbol,"kaikong",1,quote["LowerLimitPrice"],order_info='开仓') #
print(r['last_msg'])
r = pqapi.open_close(symbol,"pingkong",1,quote["LowerLimitPrice"],block=False,order_info='平仓挂单') #
print(r['last_msg'])
r = pqapi.open_close(symbol,"pingkong",1,quote["UpperLimitPrice"],order_info='再次平仓') #
print(r['last_msg'])

