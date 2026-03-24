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



TradeFrontAddr = "tcp://121.37.80.177:20002" #交易前置地址
MdFrontAddr = "tcp://121.37.80.177:20004" #行情前置地址

BROKERID="H宏源期货_主席"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID=""   #客户端ID
AUTHCODE=""  #授权码

#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
account = pqapi.get_account()            #获取账户资金
#建议先取行情后取持仓
quote_leg1 = pqapi.get_quote("m2605")        #获取第一腿合约行情
quote_leg2 = pqapi.get_quote("m2609")        #获取第二腿合约行情
position_leg1 = pqapi.get_position("m2605")   #获取第一腿合约持仓
position_leg2 = pqapi.get_position("m2609")   #获取第二腿合约持仓
symbol_info = pqapi.get_symbol_info("m2605")   #获取合约信息
SP = "SP m2605&m2609" #组合合约
position_combsp = pqapi.get_position(SP)   #获取组合合约持仓
quote_combin = pqapi.get_quote(SP)  #获取组合合约行情
position_combine = pqapi.get_comb_position(SP)  #根据两腿计算组合持仓
print("组合持仓:", position_combine)
spread = pqapi.spread_price("m2605","m2609")   #根据两腿计算价差行情
print("组合行情:", spread)
r = pqapi.open_close(SP,'kaiduo',1,spread['UpperLimitPrice']) #开多1手
print("开多,成交均价:", r['junjia']) 
position_combine = pqapi.get_comb_position(SP)  #根据两腿计算组合持仓
print("组合新持仓:", position_combine)
spread = pqapi.spread_price("m2605","m2609")   #根据两腿计算价差行情
r = pqapi.open_close(SP,'kaikong',1,spread['LowerLimitPrice']) #开空1手
print("开空,成交均价:", r['junjia']) 
position_combine = pqapi.get_comb_position(SP)  #根据两腿计算组合持仓
print("组合新持仓:", position_combine)
#合约行情时间
UpdateTime_leg1 = quote_leg1.ctp_datetime
UpdateTime_leg2 = quote_leg2.ctp_datetime

while True:
    #break
    if UpdateTime_leg1 != quote_leg1.ctp_datetime or UpdateTime_leg2 != quote_leg2.ctp_datetime: #新行情推送
        UpdateTime_leg1 = quote_leg1.ctp_datetime
        UpdateTime_leg2 = quote_leg2.ctp_datetime
        print(position_leg1.InstrumentID,position_leg1.pos_long,position_leg1.open_price_long,position_leg1.pos_short,position_leg1.open_price_short)
        print(position_leg2.InstrumentID,position_leg2.pos_long,position_leg2.open_price_long,position_leg2.pos_short,position_leg2.open_price_short)

        position_combine = pqapi.get_comb_position(SP)  #根据两腿计算组合持仓,实时调用以计算最新可组合持仓
        print("组合新持仓:", position_combine,position_combsp.InstrumentID,position_combsp.pos_long,position_combsp.open_price_long,position_combsp.pos_short,position_combsp.open_price_short)
        spread = pqapi.spread_price("m2605","m2609")   #根据两腿计算价差行情
        #对比spread和quote_combin,验证价差行情计算的正确性
        print("组合行情:", spread, quote_combin.LastPrice, quote_combin.BidPrice1, quote_combin.AskPrice1)
        print(quote_leg1.InstrumentID, quote_leg1.LastPrice, quote_leg1.BidPrice1, quote_leg1.AskPrice1)
        print(quote_leg2.InstrumentID, quote_leg2.LastPrice, quote_leg2.BidPrice1, quote_leg2.AskPrice1)
        if position_combine['pos_long'] > 0 and abs(position_combine['open_price_long'] - spread['BidPrice1']) >= 1*symbol_info.PriceTick:
            r = pqapi.open_close(SP,'pingduo',position_combine['pos_long'],spread['BidPrice1']) #平多
            print("平多,成交均价:", r['junjia'])  
        if position_combine['pos_short'] > 0 and abs(position_combine['open_price_short'] - spread['AskPrice1']) >= 1*symbol_info.PriceTick:
            r = pqapi.open_close(SP,'pingkong',position_combine['pos_short'],spread['AskPrice1']) #平空
            print("平空,成交均价:", r['junjia'])   
    tm.sleep(0.001)  #避免while空转占用过多CPU资源