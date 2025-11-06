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

def cta(symbol_ctp,lot,**kw ):
    quote1 = pqapi.get_quote(symbol_ctp)        #获取合约行情
    symbole_info = pqapi.get_symbol_info(symbol_ctp) #获取合约属性
    position1 = pqapi.get_position(symbol_ctp)   #获取合约持仓
    UpdateTime = quote1.local_timestamp #行情更新时间
    PriceTick = symbole_info['PriceTick']
    balance = kw["balance"] if "balance" in kw else 0  #账户最低权益
    risk_ratio = kw["risk_ratio"] if "risk_ratio" in kw else 1  #账户风险度
    #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、重复报单数   阈值
    orders_insert,orders_cancel,daylots,self_trade,order_exe,repeat = 4,2,500000,10000,500000,2
    while True:
        if UpdateTime != quote1.local_timestamp: #新行情推送
            UpdateTime = quote1.local_timestamp
            #权益足够,风险度足够,否则只平不开
            risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
            #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
            orderrisk = pqapi.get_order_risk(symbol_ctp)
            order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel 
                             and orderrisk["repeat"] < repeat 
                            )
            buy_up = 1  #多头信号
            sell_down = 1  #空头信号
            #策略中下单逻辑,先判断报撤单、权益、风险度阈值是否满足,再判断交易信号并下单,下单错误时退出交易
            if order_enable:#报撤单等未超出阈值,可执行下单
                if buy_up :
                    price = quote1["AskPrice1"]
                    r = pqapi.open_close(symbol_ctp,"kaiduo",lot,price,che_time=2,order_info='开仓')
                    if r['shoushu']:
                        pass
                    else:
                        if r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                elif sell_down :
                    price = quote1["BidPrice1"]
                    r = pqapi.open_close(symbol_ctp,"kaikong",lot,price,che_time=2,order_info='开仓')
                    if r['shoushu']:
                        pass
                    else:
                        if r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                if position1.pos_long :
                    price = quote1["BidPrice1"]
                    r = pqapi.open_close(symbol_ctp,"pingduo",position1.pos_long,price,che_time=2,order_info='平仓')
                    if r['shoushu']:
                        pass
                    else:
                        if r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
                if position1.pos_short :
                    price = quote1["AskPrice1"]
                    r = pqapi.open_close(symbol_ctp,"pingkong",position1.pos_short,price,che_time=2,order_info='平仓')
                    if r['shoushu']:
                        pass
                    else:
                        if r['order_wrong']: #下单错误
                            print(f"{symbol_ctp}下单错误退出策略执行,错误信息:",r['last_msg']) #错误信息
                            return
            else: #权益、风险度、报撤单等超过阈值
                if orderrisk["order_count"] >= orders_insert: print(f"{symbol_ctp},退出策略执行警告，报单笔数已超预警{orderrisk['order_count']} >= {orders_insert}")
                if orderrisk["cancel_count"] >= orders_cancel: print(f"{symbol_ctp},退出策略执行警告，撤单笔数已超预警{orderrisk['cancel_count']} >= {orders_cancel}")
                if orderrisk["repeat"] >= repeat: print(f"{symbol_ctp},退出策略执行警告，重复报单笔数已超预警{orderrisk['repeat']} >= {repeat}")   
                return
                 
'''接口适应性'''
#创建api实例,连接交易前置,登录账户
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, 
                       TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="",ProductionMode=False)
account = pqapi.get_account()  #获取账户资金

cta1 = zhuchannel.WorkThread(cta,args=('c2601',1 ),kwargs={})
cta1.start()
cta2 = zhuchannel.WorkThread(cta,args=('y2601',0.2 ),kwargs={})
cta2.start()
