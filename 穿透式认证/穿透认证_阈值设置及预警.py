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
symbol = 'ru2601'
quote = pqapi.get_quote(symbol)  #获取合约行情
symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
position = pqapi.get_position(symbol)        #获取合约持仓
#开仓,多头1手
r = pqapi.open_close(symbol,'kaiduo',1,quote["UpperLimitPrice"],order_info='验证重复开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
r = pqapi.open_close(symbol,'kaiduo',1,quote["UpperLimitPrice"],order_info='验证重复开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
r = pqapi.open_close(symbol,'kaiduo',1,quote["UpperLimitPrice"],order_info='验证重复开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
r = pqapi.open_close(symbol,'kaiduo',1,quote["UpperLimitPrice"],order_info='验证重复开仓')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")

r = pqapi.open_close(symbol,'kaikong',1,quote["UpperLimitPrice"],che_time=2,order_info='验证撤单')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")
r = pqapi.open_close(symbol,'kaikong',1,quote["UpperLimitPrice"],che_time=2,order_info='验证撤单')
print(f"下单完成,合约:{r['symbol']},交易方向{r['kaiping']},成交手数:{r['shoushu']},报单价:{r['price']},成交均价:{r['junjia']},委托单信息:{r['last_msg']},备注:{r['order_info']}")

#报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、重复报单数   阈值
orders_insert,orders_cancel,daylots,self_trade,order_exe,repeat = 4,2,1,1,1,2
#获取报撤单、信息量统计
orderrisk = pqapi.get_order_risk(symbol)
if orderrisk["order_count"] >= orders_insert: print(f"警告，报单笔数已超预警{orderrisk['order_count']} >= {orders_insert}")
if orderrisk["cancel_count"] >= orders_cancel: print(f"警告，撤单笔数已超预警{orderrisk['cancel_count']} >= {orders_cancel}")
if orderrisk["repeat"] >= repeat: print(f"警告，重复报单笔数已超预警{orderrisk['repeat']} >= {repeat}")