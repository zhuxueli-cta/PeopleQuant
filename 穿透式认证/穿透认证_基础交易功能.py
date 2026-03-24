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

#TradeFrontAddr="tcp://180.168.146.187:10101"   #交易前置地址
#MdFrontAddr="tcp://101.230.209.178:53313"      #行情前置地址
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
symbol = 'bu2601'
quote = pqapi.get_quote(symbol)  #获取合约行情
symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
position = pqapi.get_position(symbol)        #获取合约持仓
#开仓,多头1手
r = pqapi.open_close(symbol,'kaiduo',1,quote["UpperLimitPrice"],order_info='验证开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
#开仓,空头1手
r = pqapi.open_close(symbol,'kaikong',1,quote["LowerLimitPrice"],order_info='验证开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")

#平仓多头1手
r = pqapi.open_close(symbol,'pingduo',1,quote["LowerLimitPrice"],order_info='验证平仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
#平仓空头1手
r = pqapi.open_close(symbol,'pingkong',1,quote["UpperLimitPrice"],order_info='验证平仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")

#开仓,多头1手,验证撤单
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],n_price_tick=0,che_time=2,order_info='验证撤单')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}\n")


