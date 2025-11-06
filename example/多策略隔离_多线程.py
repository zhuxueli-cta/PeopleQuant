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
import json
import asyncio,threading
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

def cta(name,product_id,symbols, **kw ):
    symbol = symbols[product_id]["strategy"][name]["symbol"]
    quote1 = pqapi.get_quote(symbol)        #获取合约行情
    symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
    position1 = pqapi.get_position(symbol)   #获取合约持仓
    UpdateTime = quote1.local_timestamp #行情更新时间
    PriceTick = symbole_info['PriceTick']
    lot = symbols[product_id]["strategy"][name]["lot"] #下单手数
    balance = symbols[product_id]["strategy"][name]["balance"] #账户最低权益
    risk_ratio = symbols[product_id]["strategy"][name]["risk_ratio"] #账户风险度
    #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量       阈值
    orders_insert,orders_cancel = symbols[product_id]["orders_insert"],symbols[product_id]["orders_cancel"],
    daylots,self_trade,order_exe = symbols[product_id]["daylots"],symbols[product_id]["self_trade"],symbols[product_id]["order_exe"]
    while True:
        t = tm.time()
        if UpdateTime != quote1.local_timestamp: #新行情推送
            UpdateTime = quote1.local_timestamp
            #权益足够,风险度足够,否则只平不开
            risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
            #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
            orderrisk = pqapi.get_order_risk(symbol)
            order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and
                            orderrisk["open_volume"] < daylots and orderrisk["self_trade_count"] < self_trade and
                            orderrisk["order_exe"] < order_exe)
            buy_up = 1  #多头信号
            sell_down = 1  #空头信号
            if order_enable:
                if buy_up and _pos_dict[product_id][name][symbol]["pos_long"] < lot and risk_control:
                    price = quote1["AskPrice1"]
                    volume = lot - _pos_dict[product_id][name][symbol]["pos_long"]
                    r = pqapi.open_close(symbol,"kaiduo",volume,price,order_info='开仓')
                    print(name,r['kaiping'],r['shoushu'],r['junjia'],r["last_msg"],_pos_dict[product_id][name][symbol] ,account.float_profit,account.Commission)
                    if r['shoushu']:
                        with lock:
                            if not _pos_dict[product_id][name][symbol]["pos_long"] : #新开仓
                                _pos_dict[product_id][name][symbol]["open_price_long"] = r['junjia']
                                _pos_dict[product_id][name][symbol]["pos_long"] = r['shoushu']
                            else: #老合约增仓
                                _pos_dict[product_id][name][symbol]["open_price_long"] = (_pos_dict[product_id][name][symbol]["open_price_long"]*_pos_dict[product_id][name][symbol]["pos_long"] 
                                                                                    + r['junjia']*r['shoushu'])/(_pos_dict[product_id][name][symbol]["pos_long"]+r['shoushu'])
                                _pos_dict[product_id][name][symbol]["pos_long"] += r['shoushu']
                            pqapi.save_json(_Position_Log,_pos_dict)
                    else:
                        if r['order_wrong']: #下单错误
                            print(r['last_msg']) #错误信息
                            return
                elif sell_down and _pos_dict[product_id][name][symbol]["pos_short"] < lot and risk_control:
                    price = quote1["BidPrice1"]
                    volume = lot - _pos_dict[product_id][name][symbol]["pos_short"]
                    r = pqapi.open_close(symbol,"kaikong",volume,price,order_info='开仓')
                    print(name,r['kaiping'],r['shoushu'],r['junjia'],r["last_msg"],_pos_dict[product_id][name][symbol] ,account.float_profit,account.Commission)
                    if r['shoushu']:
                        with lock:
                            if not _pos_dict[product_id][name][symbol]["pos_short"] :
                                _pos_dict[product_id][name][symbol]["open_price_short"] = r['junjia']
                                _pos_dict[product_id][name][symbol]["pos_short"] = r['shoushu']
                            else:
                                _pos_dict[product_id][name][symbol]["open_price_short"] = (_pos_dict[product_id][name][symbol]["open_price_short"]*_pos_dict[product_id][name][symbol]["pos_short"] 
                                                                                    + r['junjia']*r['shoushu'])/(_pos_dict[product_id][name][symbol]["pos_short"]+r['shoushu'])
                                _pos_dict[product_id][name][symbol]["pos_short"] += r['shoushu']
                            pqapi.save_json(_Position_Log,_pos_dict)
                    else:
                        if r['order_wrong']: #下单错误
                            print(r['last_msg']) #错误信息
                            return
                if _pos_dict[product_id][name][symbol]["pos_long"] and abs(quote1.LastPrice - _pos_dict[product_id][name][symbol]["open_price_long"]) >= 1*PriceTick:
                    price = quote1["BidPrice1"]
                    volume = min(_pos_dict[product_id][name][symbol]["pos_long"],position1.pos_long) #不超出总仓
                    r = pqapi.open_close(symbol,"pingduo",volume,price,order_info='平仓')
                    print(name,r['kaiping'],r['shoushu'],r['junjia'],r["last_msg"],_pos_dict[product_id][name][symbol] ,account.float_profit,account.Commission)
                    if r['shoushu']:
                        with lock:
                            profit_count = r['junjia'] - _pos_dict[product_id][name][symbol]["open_price_long"] #盈利价差
                            profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                            _pos_dict[product_id][name]["profit"] += profit_money #策略利润
                            _pos_dict[product_id]["profit"] += profit_money  #品种利润
                            _pos_dict["profit"] += profit_money #总利润
                            _pos_dict[product_id][name][symbol]["pos_long"] -= r['shoushu']
                            pqapi.save_json(_Position_Log,_pos_dict)
                         
                    else:
                        if r['order_wrong']: #下单错误
                            print(r['last_msg']) #错误信息
                            return
                if _pos_dict[product_id][name][symbol]["pos_short"] and abs(quote1.LastPrice - _pos_dict[product_id][name][symbol]["open_price_short"]) >= 1*PriceTick:
                    price = quote1["AskPrice1"]
                    volume = min(_pos_dict[product_id][name][symbol]["pos_short"],position1.pos_short) #不超出总仓
                    r = pqapi.open_close(symbol,"pingkong",volume,price,order_info='平仓')
                    print(name,r['kaiping'],r['shoushu'],r['junjia'],r["last_msg"],_pos_dict[product_id][name][symbol] ,account.float_profit,account.Commission)
                    if r['shoushu']:
                        with lock:
                            profit_count = _pos_dict[product_id][name][symbol]["open_price_short"] - r['junjia'] #盈利价差
                            profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                            _pos_dict[product_id][name]["profit"] += profit_money #策略利润
                            _pos_dict[product_id]["profit"] += profit_money  #品种利润
                            _pos_dict["profit"] += profit_money #总利润
                            _pos_dict[product_id][name][symbol]["pos_short"] -= r['shoushu']
                            pqapi.save_json(_Position_Log,_pos_dict)
                         
                    else:
                        if r['order_wrong']: #下单错误
                            print(r['last_msg']) #错误信息
                            return
            else: #权益、风险度、报撤单等超过阈值
                print(account,orderrisk)    

#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
account = pqapi.get_account()            #获取账户资金
positions = pqapi.get_position()
_flog = r'C:\PositionLogs'  #创建PositionLogs目录
#print("本地数据记录路径",_flog) #输出记录路径
os.makedirs(_flog,exist_ok=True)
if os.path.isfile(_flog+r'\PositionLog.json') != True: #
    with open(_flog+r'\PositionLog.json','w+',encoding='utf-8') as _Position_Log:
        json.dump({},_Position_Log)  
    _Position_Log=open(_flog+r'\PositionLog.json','r+',encoding='utf-8') 
else : _Position_Log=open(_flog+r'\PositionLog.json','r+',encoding='utf-8') #
_pos_dict = json.load(_Position_Log)    #
if "profit" not in _pos_dict: _pos_dict["profit"] = 0  #添加平仓盈亏
symbols = { 
            "hc":{"strategy":{"cta1":{"symbol":"hc2601","balance":0, "risk_ratio":0.4,  "lot":5,},
                              "cta2":{"symbol":"hc2601", "balance":0, "risk_ratio":0.6,   "lot":1,},
                            },
                    "orders_insert":5000,"orders_cancel":5000,"daylots":5000,"self_trade":5000,"order_exe":5000,
                },
        }
lock = threading.Lock()
for product_id in symbols :
    if "strategy" in symbols[product_id]:   
        for name in symbols[product_id]["strategy"]:
            
            d = {name:{"profit":0,symbols[product_id]["strategy"][name]["symbol"]:{"open_price_long":float('nan'),"pos_long":0, "open_price_short":float('nan'),"pos_short":0,}}
                }
            if product_id not in _pos_dict: _pos_dict.update({product_id: d })
            if "profit" not in _pos_dict[product_id]: _pos_dict[product_id]["profit"] = 0
            if name not in _pos_dict[product_id]: _pos_dict[product_id].update(d)
            if name == "cta1":
                cta1 = zhuchannel.WorkThread(cta,args=('cta1',product_id,symbols  ),kwargs={})
                cta1.start()
            if name == "cta2":
                cta2 = zhuchannel.WorkThread(cta,args=('cta2',product_id,symbols ),kwargs={})
                cta2.start()

local_timestamp = account.local_timestamp
while True:
    if local_timestamp != account.local_timestamp:
        local_timestamp = account.local_timestamp
        #print(account.PositionProfit,account.float_profit,"\n")
        #print(positions)
        #print(pqapi.get_symbol_commission('FG601'))
    tm.sleep(10)
