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
symbol = 'm2601'
quote = pqapi.get_quote(symbol)  #获取合约行情
symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
position = pqapi.get_position(symbol)        #获取合约持仓
#开仓,多头1手
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],block=False,order_info='验证批量撤单')
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],block=False,order_info='验证批量撤单')
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],block=False,order_info='验证批量撤单')
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],block=False,order_info='验证批量撤单')
r = pqapi.open_close(symbol,'kaiduo',1,quote["LowerLimitPrice"],block=False,order_info='验证批量撤单')
'''批量撤单'''
pqapi.cancel_all_order(symbol) #撤合约symbol的未成交单
print("批量撤单完成")
