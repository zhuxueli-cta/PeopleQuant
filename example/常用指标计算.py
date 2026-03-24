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
import asyncio
import traceback
import types
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any
from peoplequant.indicators import MA_df, MACD_df, BOLL_df
from peoplequant.indicators import MA_expr, MACD_expr, BOLL_expr,CROSSUP_expr,CROSSDOWN_expr


envs = {
    "7x24": {
        "td": "tcp://182.254.243.31:40001",
        "md": "tcp://182.254.243.31:40011",
    },
    "电信1": {
        "td": "tcp://182.254.243.31:30001",
        "md": "tcp://182.254.243.31:30011",
    },
    "电信2": {
        "td": "tcp://182.254.243.31:30002",
        "md": "tcp://182.254.243.31:30012",
    },
    "移动": {
        "td": "tcp://182.254.243.31:30003",
        "md": "tcp://182.254.243.31:30013",
    },
}
TradeFrontAddr = envs["电信1"]["td"]
MdFrontAddr = envs["电信1"]["md"]
#TradeFrontAddr = envs["7x24"]["td"]
#MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"   #期货公司ID
USERID=""   #账户
PASSWORD=""   #登录密码
APPID="simnow_client_test"   #客户端ID
AUTHCODE="0000000000000000"  #授权码

#创建api实例
pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="",
                       storage_format="parquet")
account = pqapi.get_account()            #获取账户资金
posions = pqapi.get_position()
symbol = "rb2605"
quote1 = pqapi.get_quote(symbol)        #获取合约行情
symbol_info = pqapi.get_symbol_info(symbol) #获取合约属性

local_timestamp = quote1.local_timestamp

kline = pqapi.get_kline(symbol, "1m", 5)   #获取K线
tick = pqapi.get_tick(symbol)                #获取Tick
kline2 = pqapi.get_kline(symbol, "1m", 50)   #获取K线
#第一种方法,对K线数据一个个计算指标,生成每个指标的数据表格
ma = MA_df(kline2.data.tail(20), 5)
macd = MACD_df(kline2.data.tail(30), 12, 26, 9)
boll = BOLL_df(kline2.data.tail(20), 5, 2)
print(ma)
print(macd)
print(boll)
print(boll.tail(2)) #取最后的2行
print(boll.tail(1)["mid"][0]) #取最新的1行,mid的值
print(boll[-1]["mid"][0]) #取最新的1行,mid的值
print(boll["mid"][-1]) #取mid列的最新的值
print(boll.head(2)["mid"][0]) #取开始的2行,mid的第一个值
print(kline2.data.select(["InstrumentID", "open", "high","low", "close", "Volume",]))
#第二种方法,传入各指标表达式,向量化计算,速度更快,所有指标合并成一个表格
#计算指标、甚至可以把部分指标信号一并计算
indicator_signal = (kline2.data.tail(30)
                    #计算指标值
                    .with_columns([
                                    MA_expr("close", 5).alias("ma5"),  #计算5日均线,m5为列名
                                    MA_expr("close", 10).alias("ma10"),  #计算10日均线,m10为列名
                                    MACD_expr("close", 12, 26, 9).alias("macd"), #计算MACD,临时列结构macd,包含三个字段diff,dea,bar
                                    BOLL_expr("close", 5, 2).alias("boll")    #计算BOLL,临时列结构为boll,包含三个字段top,mid,bottom
                                ]).unnest("macd").unnest("boll") #展开MACD和BOLL到单列
                    #计算信号
                    #.with_columns([
                    #                CROSSUP_expr("ma5", "ma10").alias("crossup"),  #判断是否金叉
                    #                CROSSDOWN_expr("ma5", "ma10").alias("crossdown"),  #判断是否死叉
                    #])
                    )
while True:
    #break
    if local_timestamp != quote1.local_timestamp: #新行情推送
        local_timestamp = quote1.local_timestamp
        #计算指标及信号
        indicator_signal = (kline2.data.tail(30)
                            .with_columns([
                                            MA_expr("close", 5).alias("ma5"),  #计算5日均线,m5为列名
                                            MA_expr("close", 10).alias("ma10"),  #计算10日均线,m10为列名
                                            MACD_expr("close", 12, 26, 9).alias("macd"), #计算MACD,临时列结构macd,包含三个字段diff,dea,bar
                                            BOLL_expr("close", 5, 2).alias("boll")    #计算BOLL,临时列结构为boll,包含三个字段top,mid,bottom
                                        ]).unnest("macd").unnest("boll") #展开MACD和BOLL到单列
                            )
        
        #不支持负索引
        print(indicator_signal.tail(2)) #取最后的2行
        print(indicator_signal.tail(1)["mid"][0]) #取最新的1行,布林线mid的值
        print(indicator_signal.head(2)["ma5"][0]) #取开始的2行,ma5的第一个值
        #5日均线金叉10日均线
        crossup = indicator_signal['ma5'][-1] > indicator_signal['ma10'][-1] and indicator_signal['ma5'][-2] <= indicator_signal['ma10'][-2]
        #5日均线死叉10日均线
        crossdown = indicator_signal['ma5'][-1] < indicator_signal['ma10'][-1] and indicator_signal['ma5'][-2] >= indicator_signal['ma10'][-2]