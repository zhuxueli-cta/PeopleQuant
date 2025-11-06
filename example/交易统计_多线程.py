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

def cta(name,symbol_ctp,filename,logfile,**kw ):
    quote1 = pqapi.get_quote(symbol_ctp)        #获取合约行情
    symbole_info = pqapi.get_symbol_info(symbol_ctp) #获取合约属性
    position1 = pqapi.get_position(symbol_ctp)   #获取合约持仓
    UpdateTime = quote1.ctp_datetime #行情更新时间
    PriceTick = symbole_info['PriceTick']
    lot = 5 #下单手数
    balance = kw["balance"] if "balance" in kw else 0  #账户最低权益
    risk_ratio = kw["risk_ratio"] if "risk_ratio" in kw else 1  #账户风险度
    #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量       阈值
    orders_insert,orders_cancel,daylots,self_trade,order_exe = 50000,50000,100000,10000,100000
    while True:
        t = tm.time()
        if UpdateTime != quote1.ctp_datetime: #新行情推送
            UpdateTime = quote1.ctp_datetime
            #权益足够,风险度足够,否则只平不开
            risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
            orderrisk = pqapi.get_order_risk(symbol_ctp)
            order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and
                            orderrisk["open_volume"] < daylots and orderrisk["self_trade_count"] < self_trade and
                            orderrisk["order_exe"] < order_exe)
            buy_up = 1  #多头信号
            sell_down = 1  #空头信号
            if order_enable:
                if buy_up and not position1.pos_long and risk_control:
                    price = quote1["AskPrice1"]
                    r = pqapi.open_close(symbol_ctp,"kaiduo",lot,price,order_info='开仓')
                    if r['shoushu']:
                        new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol_ctp,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':0,'利润金额':0, '日期':f'{UpdateTime}'}])
                        pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                elif sell_down and not position1.pos_short and risk_control:
                    price = quote1["BidPrice1"]
                    r = pqapi.open_close(symbol_ctp,"kaikong",lot,price,order_info='开仓')
                    if r['shoushu']:
                        new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol_ctp,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':0,'利润金额':0, '日期':f'{UpdateTime}'}])
                        pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                if position1.pos_long and abs(quote1.LastPrice - position1.open_price_long) >= 1*PriceTick:
                    price = quote1["BidPrice1"]
                    open_price_long = position1.open_price_long  #初始开仓价位
                    r = pqapi.open_close(symbol_ctp,"pingduo",position1.pos_long,price,order_info='平仓')
                    if r['shoushu']:
                        profit_count = r['junjia'] - open_price_long #盈利价差
                        profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                        new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol_ctp,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':profit_count,'利润金额':profit_money, '日期':f'{UpdateTime}'}])
                        pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                if position1.pos_short and abs(quote1.LastPrice - position1.open_price_short) >= 1*PriceTick:
                    price = quote1["AskPrice1"]
                    open_price_short = position1.open_price_short  #初始开仓价位
                    r = pqapi.open_close(symbol_ctp,"pingkong",position1.pos_short,price,order_info='平仓')
                    if r['shoushu']:
                        profit_count = open_price_short - r['junjia']  #盈利价差
                        profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                        new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol_ctp,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':profit_count,'利润金额':profit_money, '日期':f'{UpdateTime}'}])
                        pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                    
   
#创建api实例
pqapi = PeopleQuantApi()
#获取账户资金
account = pqapi.get_account()            

cta1 = zhuchannel.WorkThread(cta,args=('热卷测试','hc2601','程序1',r'C:\CTPLogs' ),kwargs={})
#cta2 = zhuchannel.WorkThread(cta,args=('al2511', ),kwargs={})
cta3 = zhuchannel.WorkThread(cta,args=('豆油测试','y2601', '程序1',r'C:\CTPLogs'),kwargs={})
cta1.start()
#cta2.start()
cta3.start()

local_timestamp = account.local_timestamp
while True:
    if local_timestamp != account.local_timestamp:
        local_timestamp = account.local_timestamp
        print(account,"\n")
    tm.sleep(5)
