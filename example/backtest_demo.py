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
from peoplequant import zhuchannel
from peoplequant.backtest import BackTest
from peoplequant.indicators import MA_expr,CROSSDOWN_expr,CROSSUP_expr
import asyncio
import traceback
import types
import polars
import time as tm
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, time, date, timedelta,timezone

#策略函数
def cta(symbol_ctp,period='5m',loss=15,profit=150 ):
    quote = pqapi.get_quote(symbol_ctp)        #获取合约行情
    symbol_info = pqapi.get_symbol_info(symbol_ctp)
    position = pqapi.get_position(symbol_ctp)   #获取合约持仓
    UpdateTime = quote.ctp_datetime #行情更新时间
    kline = pqapi.get_kline(symbol_ctp,period=period,kline_count=40) #获取K线数据
    lot = 1 #下单手数
    signals = [] #记录交易信号,用于交易逻辑分析
    Data = polars.DataFrame()
    #添加新行情是否被使用记录
    quote_lable = pqapi.update_quote_pipe([symbol_ctp],add_or_used='add')
    while True:
        if pqapi.backtest_finished :
            #回测结束,将记录的交易信号保存在本地
            polars.DataFrame(signals).write_csv(fr'{pqapi._backtest.result_file}\backtest_signal_{sum(quote_lable)}.csv')
            #Data.write_csv(fr'{pqapi._backtest.result_file}\backtest_indicator.csv')
            print('回测结束')
            print(f"回测结果已保存至:{pqapi._backtest.result_file}")
            break
        if UpdateTime != quote.ctp_datetime and not kline.data.is_empty(): #新行情推送
            
            UpdateTime = quote.ctp_datetime
            #转换为pandas.DataFrame
            #kline_pandas = kline.data.to_pandas()

            #计算指标，10日/5日均线，均价金叉死叉
            indicator_signal = (kline.data.tail(30)
                    .with_columns([
                                    #MA_expr("close", 5).alias("ma5"),  #计算5日均线,m5为列名
                                    MA_expr("close", 10).alias("ma10"),  #计算10日均线,m10为列名
                                ]) 
                    #.with_columns([
                    #                CROSSUP_expr("ma5", "ma10").alias("crossup"),  #判断是否金叉
                    #                CROSSDOWN_expr("ma5", "ma10").alias("crossdown"),  #判断是否死叉
                    #]) 
                    )
            #生成开仓条件
            buy_up = indicator_signal.height >= 2 and quote.LastPrice > indicator_signal['ma10'][-1] > indicator_signal['ma10'][-2] and quote.LastPrice > indicator_signal['open'][-1]  #多头信号
            sell_down = indicator_signal.height >= 2 and quote.LastPrice < indicator_signal['ma10'][-1] < indicator_signal['ma10'][-2] and quote.LastPrice < indicator_signal['open'][-1]  #空头信号
            
            loss_long = quote.LastPrice < position.open_price_long - loss*symbol_info.PriceTick
            loss_short = quote.LastPrice > position.open_price_short + loss*symbol_info.PriceTick
            profit_long = quote.LastPrice > position.open_price_long + profit*symbol_info.PriceTick and quote.LastPrice < indicator_signal['ma10'][-1]
            profit_short = quote.LastPrice < position.open_price_short - profit*symbol_info.PriceTick and quote.LastPrice > indicator_signal['ma10'][-1]
            #记录全部指标计算
            #if Data.height < indicator_signal.height: Data = Data.vstack(indicator_signal).unique(subset=['kline_date'],keep='last').sort('ctp_datetime')
            #else: Data = Data.vstack(indicator_signal.tail(1)).unique(subset=['kline_date'],keep='last').sort('ctp_datetime')
            
            if buy_up and not position.pos_short + position.pos_long:
                price = quote.LastPrice #下单价格
                #将时间、价格、持仓等参与交易逻辑计算的数据记录下来
                signals.append({"ctp_datetime":quote.ctp_datetime,"LastPrice":quote.LastPrice,"kline_close":(kline.data)["close"][-1],
                                "pos_long":position.pos_long,"open_price_long":position.open_price_long,"float_profit_long":position.float_profit_long,
                                "pos_short":position.pos_short,"open_price_short":position.open_price_short,"float_profit_short":position.float_profit_short,
                                "buy_up":buy_up,"sell_down":sell_down,'loss_long':loss_long,'loss_short':loss_short,'profit_long':profit_long,
                                'profit_short':profit_short,'open_price_long+':position.open_price_long + 150*symbol_info.PriceTick,'open_price_long-':position.open_price_long - 50*symbol_info.PriceTick,
                                'open_price_short+':position.open_price_short + 50*symbol_info.PriceTick,'open_price_short-':position.open_price_short - 150*symbol_info.PriceTick,
                                "PriceTick":symbol_info.PriceTick,'ma10':indicator_signal['ma10'][-1]})
                #下单
                r = pqapi.open_close(symbol_ctp,"kaiduo",lot,price)
                #print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position.open_price_long,position.open_price_short)
            elif sell_down and not position.pos_short + position.pos_long:
                price = quote.LastPrice
                signals.append({"ctp_datetime":quote.ctp_datetime,"LastPrice":quote.LastPrice,"kline_close":(kline.data)["close"][-1],
                                "pos_long":position.pos_long,"open_price_long":position.open_price_long,"float_profit_long":position.float_profit_long,
                                "pos_short":position.pos_short,"open_price_short":position.open_price_short,"float_profit_short":position.float_profit_short,
                                "buy_up":buy_up,"sell_down":sell_down,'loss_long':loss_long,'loss_short':loss_short,'profit_long':profit_long,
                                'profit_short':profit_short,'open_price_long+':position.open_price_long + 150*symbol_info.PriceTick,'open_price_long-':position.open_price_long - 50*symbol_info.PriceTick,
                                'open_price_short+':position.open_price_short + 50*symbol_info.PriceTick,'open_price_short-':position.open_price_short - 150*symbol_info.PriceTick,
                                "PriceTick":symbol_info.PriceTick,'ma10':indicator_signal['ma10'][-1]})
                r = pqapi.open_close(symbol_ctp,"kaikong",lot,price)
                #print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position.open_price_long,position.open_price_short)
            #根据最新持仓计算止盈止损条件
            loss_long = quote.LastPrice < position.open_price_long - 50*symbol_info.PriceTick
            loss_short = quote.LastPrice > position.open_price_short + 50*symbol_info.PriceTick
            profit_long = quote.LastPrice > position.open_price_long + 150*symbol_info.PriceTick and quote.LastPrice < indicator_signal['ma10'][-1]
            profit_short = quote.LastPrice < position.open_price_short - 150*symbol_info.PriceTick and quote.LastPrice > indicator_signal['ma10'][-1]
            if position.pos_long and (loss_long or profit_long) :
                price = quote.LastPrice
                signals.append({"ctp_datetime":quote.ctp_datetime,"LastPrice":quote.LastPrice,"kline_close":(kline.data)["close"][-1],
                                "pos_long":position.pos_long,"open_price_long":position.open_price_long,"float_profit_long":position.float_profit_long,
                                "pos_short":position.pos_short,"open_price_short":position.open_price_short,"float_profit_short":position.float_profit_short,
                                "buy_up":buy_up,"sell_down":sell_down,'loss_long':loss_long,'loss_short':loss_short,'profit_long':profit_long,
                                'profit_short':profit_short,'open_price_long+':position.open_price_long + 150*symbol_info.PriceTick,'open_price_long-':position.open_price_long - 50*symbol_info.PriceTick,
                                'open_price_short+':position.open_price_short + 50*symbol_info.PriceTick,'open_price_short-':position.open_price_short - 150*symbol_info.PriceTick,
                                "PriceTick":symbol_info.PriceTick,'ma10':indicator_signal['ma10'][-1]})
                r = pqapi.open_close(symbol_ctp,"pingduo",position.pos_long,price)
                #print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position.open_price_long,position.pos_long,position.pos_short,position.open_price_short)
            if position.pos_short and (loss_short or profit_short) :
                price = quote.LastPrice
                signals.append({"ctp_datetime":quote.ctp_datetime,"LastPrice":quote.LastPrice,"kline_close":(kline.data)["close"][-1],
                                "pos_long":position.pos_long,"open_price_long":position.open_price_long,"float_profit_long":position.float_profit_long,
                                "pos_short":position.pos_short,"open_price_short":position.open_price_short,"float_profit_short":position.float_profit_short,
                                "buy_up":buy_up,"sell_down":sell_down,'loss_long':loss_long,'loss_short':loss_short,'profit_long':profit_long,
                                'profit_short':profit_short,'open_price_long+':position.open_price_long + 150*symbol_info.PriceTick,'open_price_long-':position.open_price_long - 50*symbol_info.PriceTick,
                                'open_price_short+':position.open_price_short + 50*symbol_info.PriceTick,'open_price_short-':position.open_price_short - 150*symbol_info.PriceTick,
                                "PriceTick":symbol_info.PriceTick,'ma10':indicator_signal['ma10'][-1]})
                r = pqapi.open_close(symbol_ctp,"pingkong",position.pos_short,price)
                #print(r['kaiping'],r['price'],r['shoushu'],r['junjia'],position.open_price_long,position.open_price_short)
        pqapi.update_quote_pipe([symbol_ctp],quote_lable,add_or_used='used') #新行情已经被使用完毕
        tm.sleep(0.0001) #避免while True空转耗费CPU

if __name__ == '__main__':
    from multiprocessing import freeze_support, current_process
    freeze_support()  
    back_test = BackTest([r'C:\datas\KQ.m@DCE.m.tick.csv',
                        #r'C:\datas\KQ.m@SHFE.rb.tick.csv',
                        ],start_datetime=datetime(2021,10,8,9,0,0),end_datetime=datetime(2026,7,8,15,0,0), balance=1000000.0*1000,
                        symbol_infos={'KQ.m@DCE.m':{'ExchangeID': "DCE", 'PriceTick':1, "VolumeMultiple":10, "OpenRatioByMoney":0.1, "OpenRatioByVolume":10,"LimitPriceRatio":0.1},
                                    #'KQ.m@SHFE.rb':{'ExchangeID': "SHFE", 'PriceTick':1, "VolumeMultiple":10, "OpenRatioByMoney":0.1, "OpenRatioByVolume":10,"LimitPriceRatio":0.1},
                                    },
                        col_mapping = { "datetime": "datetime","InstrumentID": "symbol","LastPrice": "close","AskPrice1": "","BidPrice1": "","AskVolume1": "","BidVolume1": "","Volume": "volume" },
                        datetime_format = "%Y-%m-%d %H:%M:%S%.f",
                        result_file=r'C:\datas',
                        draw_line = True)                
    #创建api实例
    pqapi = PeopleQuantApi(BrokerID='', UserID='', PassWord='', save_tick=False,storage_format='csv',
                        backtest=back_test)
    account = pqapi.get_account()

    #创建策略线程
    cta1 = zhuchannel.WorkThread(cta,args=('KQ.m@DCE.m','1d' ),kwargs={'loss':30,'profit':60 })
    cta1.start()
    #cta2 = zhuchannel.WorkThread(cta,args=('KQ.m@SHFE.rb','1h' ),kwargs={'loss':15,'profit':150 })
    #cta2.start()
    #print("✅ 回测完成，主线程退出")