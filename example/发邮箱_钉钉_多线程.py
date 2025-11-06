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
import os,sys
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
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信1": {
        "td": "tcp://182.254.243.31:30001",
        "md": "tcp://182.254.243.31:30011",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "电信2": {
        "td": "tcp://182.254.243.31:30002",
        "md": "tcp://182.254.243.31:30012",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
    "移动": {
        "td": "tcp://182.254.243.31:30003",
        "md": "tcp://182.254.243.31:30013",
        "user_id": "",
        "password": "",
        "broker_id": "9999",
        "authcode": "0000000000000000",
        "appid": "simnow_client_test",
        "user_product_info": "",
    },
}
TradeFrontAddr="tcp://180.168.146.187:10101"   #交易前置地址
MdFrontAddr="tcp://101.230.209.178:53313"      #行情前置地址
TradeFrontAddr = envs["电信2"]["td"]
MdFrontAddr = envs["电信2"]["md"]
#TradeFrontAddr = envs["7x24"]["td"]
#MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

'''接口适应性'''
#创建api实例,连接交易前置,登录账户
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, 
                       TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="",ProductionMode=True)
account = pqapi.get_account()  #获取账户资金
symbol = 'bu2601'
quote = pqapi.get_quote(symbol)  #获取合约行情
symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
position = pqapi.get_position(symbol)        #获取合约持仓
DingDing,WeChat = [],[]
QQemail = ["123576@qq.com","sgh323","123576@qq.com","穿透式测试"]
chan = zhuchannel.ThreadChan()
send_msg_task = zhuchannel.WorkThread(pqapi.send_msg,args=(DingDing,WeChat,QQemail, chan ),kwargs={"logfile":pqapi._logfile,"_print":False, "sf": ""})
send_msg_task.start()

balance = 100  #账户最低权益
risk_ratio = 0.8  #账户风险度
#报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、重复报单数   阈值
orders_insert,orders_cancel,daylots,self_trade,order_exe,repeat = 1,1,1,1,1,10
#权益足够,风险度足够,否则只平不开
risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
#报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
orderrisk = pqapi.get_order_risk(symbol)
order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and 
                orderrisk["open_volume"] < daylots and orderrisk["self_trade_count"] < self_trade and
                orderrisk["order_exe"] < order_exe)

#策略中下单逻辑,先判断报撤单、权益、风险度阈值是否满足,再判断交易信号并下单,下单错误时退出交易
if order_enable: #报撤单等未超出阈值,可执行下单
    if not position.pos_long and risk_control: #无仓时开仓,资金权益、风险度未超阈值,可执行下单
        price = quote["AskPrice1"]
        r = pqapi.open_close(symbol,"kaiduo",1,price,order_info='开仓')
        if r['shoushu']: #有成交
            print(f"下单完成,合约:{r['symbol']},成交手数:{r['shoushu']},成交均价:{r['junjia']}")
        else: #未成交
            if r['order_wrong']: #下单错误
                print("柜台或交易所拒单,避免频繁报撤单,停止交易")
            print(r['last_msg']) #错误信息
else: #权益、风险度、报撤单等超过阈值
    print(f'超出风控阈值,账户权益:{account["Balance"]},风险度:{account["risk_ratio"]},报撤单统计:{orderrisk}\n')  
    pqapi.send_message(chan, account, orderrisk, name="test",e="超出风控阈值")  #发邮箱提醒


