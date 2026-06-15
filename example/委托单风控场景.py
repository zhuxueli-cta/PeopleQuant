#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.insert(0, root_path)
from peoplequant.pqctp import PeopleQuantApi 
import time as tm
import asyncio
import traceback
import types
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any
import random

TradeFrontAddr = ''
MdFrontAddr = ''
BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")

symbol = 'rb2605'
quote = pqapi.get_quote(symbol) #获取合约行情
symbol_info = pqapi.get_symbol_info(symbol) #获取合约属性
positons = pqapi.get_position(symbol) #获取合约持仓
#大单拆分
lots = 1000
#多头持仓小于目标数量
while positons.pos_long < lots: #循环执行直到持仓开完
    lot = random.randrange(10, 100) #在10~100之间随机生成下单数量
    lot = min(lot, lots - positons.pos_long) #剩余下单数量
    #默认按超价1跳下单,排队价偏离1跳后自动撤单,重新追单
    r = pqapi.open_close(symbol,"kaiduo",lot,price='超价',n_price_tick=1,order_info='大单拆分')
    lots -= r['shoushu']

#自成交检查
kaiping ="kaiduo" #交易方向,买
price = quote.AskPrice1 #买入价格
direction = 'Sell' if kaiping in ["kaiduo","pingkong"] else 'buy'
#查询相反方向未成交委托单
orders = pqapi.get_symbol_order(symbol,OrderStatus="Alive",Direction=direction)
for od in orders:
    if kaiping in ["kaiduo","pingkong"] and price >= od["LimitPrice"] == quote.AskPrice1:
        '''买入价格等于对手价,对手价有相反方向未成交挂单,存在自成交可能,则不下单'''

#阈值设置
#报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、重复报单数   阈值
orders_insert,orders_cancel,daylots,self_trade,order_exe,repeat = 4,2,1,1,1,2
#获取报撤单、信息量统计
orderrisk = pqapi.get_order_risk(symbol)
if orderrisk["order_count"] >= orders_insert: print(f"警告，报单笔数已超预警{orderrisk['order_count']} >= {orders_insert}")
if orderrisk["cancel_count"] >= orders_cancel: print(f"警告，撤单笔数已超预警{orderrisk['cancel_count']} >= {orders_cancel}")
if orderrisk["repeat"] >= repeat: print(f"警告，重复报单笔数已超预警{orderrisk['repeat']} >= {repeat}")

#策略阈值
day_cancel,day_order = 0,0 #初始值
pqapi.open_close("FG610",'kaiduo',1,quote.LowerLimitPrice,n_price_tick = 1,OrderMemo='')
day_order += r['day_order']
day_cancel += r['che_count']
