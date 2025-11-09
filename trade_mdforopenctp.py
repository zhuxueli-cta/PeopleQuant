#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from peoplequant.libs import ctpapi
import time as tm
from peoplequant import zhuchannel
import threading,queue
import os
import asyncio
import requests
import smtplib                                 # smtp服务器
from email.mime.text import MIMEText#发送文本
from email.mime.multipart import MIMEMultipart#生成多个部分的邮件体
from email.mime.application import MIMEApplication#发送图片
from concurrent.futures import ThreadPoolExecutor
import traceback,json
import types
from functools import partial
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any, Union, Callable, Set
from peoplequant.zhustruct import Quote,Account,Position,Trade,Order,InstrumentProperty,CTPDataProcessor,MutableKlineHolder,MutableTickHolder

class PeopleQuantApi():
    def __init__(self,BrokerID:str,UserID:str,PassWord:str,AppID:str="", AuthCode:str="",TradeFrontAddr:str="",MdFrontAddr:str="",
                 s:str="",vip_code="",flowfile="" ,quote_queue:zhuchannel.ThreadChan=None,save_tick=False,ProductionMode=True,
                 _FTDmaxsize={"Session":6,"ReqQry":1,"OrderInsert":6,"OrderAction":6,"order_exe":10,"ex_order_exe":10},**kw):
        '''
        BrokerID:期货公司代码
        UserID:期货账号
        PassWord:账号密码
        AppID:穿透式认证程序编号
        AuthCode:穿透式认证授权码
        TradeFrontAddr:交易前置地址
        MdFrontAddr:行情前置地址
        s:日志文件夹名称
        flowfile:数据流文件保存目录,默认在当前程序目录下创建
        quote_queue:行情队列,用于从CTP行情接口接收行情,推送向策略层(策略层若需要CTP行情)
        save_tick:是否保存tick到本地
        ProductionMode:True:生产模式,False:测试模式
        '''
        self._subscribequeue = zhuchannel.ThreadChan() #mdapi从各层包括tradeapi接收订阅、退订
        self._quote_queue = zhuchannel.ThreadChan() #mdapi向tradeapi发送行情
        self.quote_queue = quote_queue #各层从mdapi接收行情
        self._Reqqueue = zhuchannel.ThreadChan() #请求队列
        self.MarketDataqueue = []
        if self.quote_queue is not None:
            self.MarketDataqueue.append(quote_queue)
        self.instruments = set()  #全部合约代码
        self.quote = Quote()  #全部合约行情
        self.instruments_property = {} #合约属性
        self.positions = Position()  #全部合约持仓
        self.orders = Order()  #全部委托单
        self.trades = Trade()  #全部成交单
        self.account= Account()  #账户资金
        self.subscribe_instruments = set() #已订阅合约代码
        self.unsubscribe_instruments = set() #已退订合约代码
        self._exception_queue = zhuchannel.ThreadChan()  # 用于传递异常的队列
        self._rtn_queue = zhuchannel.ThreadChan()  # 用于接收CTP主动发出的回报队列
        self._ans_queue = zhuchannel.ThreadChan( )  # 用于接收CTP反馈的结果队列
        self._ReqID = 0  #请求编号
        self._save_tick = save_tick
        self._save_tick_symbols = {}
        self._save_tick_queue = zhuchannel.ThreadChan( )  # 用于保存ticks队列
        self._pos_queue :Dict[str, Union[asyncio.Queue,queue.Queue]] = {}
        self._FTDmaxsize = _FTDmaxsize
        if not flowfile:
            try:
                self._flowfile = os.path.dirname(os.path.abspath(__file__)) #当前程序目录(该py文件__file__所在目录)
            except: self._flowfile = os.getcwd()   #程序工作目录下创建目录 
        else: self._flowfile = flowfile
        self._logfile = fr"{self._flowfile}\logs\{BrokerID}\{UserID}\{s}"
        self.CTP_data_processor = CTPDataProcessor(_logfile=self._logfile)

        self._tradekwargs = {"BrokerID":BrokerID,"UserID":UserID,"Password":PassWord,"AppID":AppID, "AuthCode":AuthCode,"s":s,"flowfile":self._flowfile,"Subscribequeue":self._subscribequeue,
                             "quote_queue":self._quote_queue,"Reqqueue":self._Reqqueue,"Notify":{"rtn_queue":self._rtn_queue,"ans_queue":self._ans_queue},"ProductionMode":ProductionMode,
                             "TradeFrontAddr":TradeFrontAddr,"PQexception_queue":self._exception_queue,"MarketDataqueue":self.MarketDataqueue,"_FTDmaxsize":self._FTDmaxsize,"vip_code":vip_code,**kw}
        self._mdkwargs = {"BrokerID":BrokerID,"UserID":UserID,"Password":PassWord,"AppID":AppID, "AuthCode":AuthCode,"s":s,"flowfile":self._flowfile,"Subscribequeue":self._subscribequeue,
                          "quote_queue":self._quote_queue,"Notify":{"rtn_queue":self._rtn_queue,"ans_queue":self._ans_queue},"ProductionMode":ProductionMode,"_FTDmaxsize":self._FTDmaxsize,
                          "MdFrontAddr":MdFrontAddr,"vip_code":vip_code,**kw}
        
        self.tradethread = zhuchannel.WorkThread(target=ctpapi.TraderApi,args=tuple(),kwargs=self._tradekwargs,_api=True,name_prefix="tradethread")
        self.mdthread = zhuchannel.WorkThread(target=ctpapi.MdApi,args=tuple(),kwargs=self._mdkwargs,_api=True,name_prefix="mdthread")
        self.tradethread.start()
        self.mdthread.start()
        self.rtnthread = zhuchannel.WorkThread(target=self._rtn_thread,args=tuple(),kwargs={},exception_queue=self._exception_queue )
        self.rtnthread.start()
        self.update_tick_kline = zhuchannel.WorkThread(target=self._update_tick_kline,args=tuple(),kwargs={},exception_queue=self._exception_queue )
        self.update_tick_kline.start()
        self.exception = zhuchannel.WorkThread(target=self.exception_thread,args=tuple(),kwargs={} )
        self.exception.start()
        self.executor = ThreadPoolExecutor()
        try:
            r1 = self._ans_queue.get(timeout=120) #等待交易初始化完成
            r2 = self._ans_queue.get(timeout=120) #等待行情初始化完成
            #self.TradingDay = self._get_trading_day()
            print("交易接口,行情接口:初始化完成")
        except Exception :
            e = "初始化失败,可能网络连接超时,请检查网络"
            self._exception_queue.put(e)
            raise Exception(e)
    def Join(self):
        self.tradethread._Join()
        self.mdthread._Join()
    def exception_thread(self):
        for e in self._exception_queue:
            e = f'\n{datetime.today()}{"-"*30}+\n{e}\n'
            self.logs_txt(e,self.tradethread.api._logfile)
            if self.rtnthread.is_alive(): self._rtn_queue.put("exception")
            self.Join()
            self.tradethread.join()
            self.mdthread.join()
            if 0:
                print(self.tradethread.is_alive(),self.mdthread.is_alive())
                threads = threading.enumerate()
                for idx, thread in enumerate(threads):
                    print(f"线程 {idx + 1}:")
                    print(f"  名称: {thread.name}")
                    print(f"  ID: {thread.ident}")  # 线程ID（可能为None，未启动时）
                    print(f"  是否存活: {thread.is_alive()}")
                    print(f"  是否为守护线程: {thread.daemon}")
                #raise Exception(e)
                for t in self.threads:
                        t.join(timeout=1.0)
                        if t.is_alive():
                            print(f"线程 {t.name} 无法优雅终止，已强制中断")
                        else:
                            print(t.is_alive(),t.name,"死了")
    
    def _get_trading_day(self,wait_return=False) -> str:
        '''查询当前交易日'''
        r = self._sendReq({"reqfuncname":"_get_trading_day","wait_return":wait_return, })
        if not r['ret'] : print("查询交易日失败")
        else: return r['ret']

    def get_account(self,wait_return=False) -> Account:
        '''
        获取账户信息
        Return:
            账户字典
        '''
        if self.account: return self.account
        else:
            r = self._sendReq({"reqfuncname":"get_account","wait_return":wait_return, })
            if not r['ret'] : 
                e = f"{datetime.now()} -get_account查询账户失败\n"
                self.logs_txt(e,self._logfile,_print=True)
            else: 
                self.account.update( r['ret'] )
                return self.account
            
    def get_position(self,InstrumentID:str="",wait_return=False,_ctp = None) -> Union[Dict[str,Position] ,Position]:
        '''
        获取品种持仓,本地计算
        Args:
            InstrumentID: 合约码
        Return:
            若查询不到返回缺省值
            返回合约的持仓字典,包含健:
                "pos_long", "pos_long_today", "pos_long_his","open_price_long" ,"position_price_long",
                "pos_short", "pos_short_today", "pos_short_his","open_price_short","position_price_short",
                "position_profit_long","float_profit_long","position_profit_short",
                "float_profit_short","margin","exch_margin",
                "margin_long","margin_short","margin_rate_long","margin_rate_short",
                "margin_volume_long","margin_volume_short",
                "instrument_id","exchange_id","ins_class","strike_price",
                "short_frozen_today","long_frozen_today","short_frozen_his","long_frozen_his",
                "OpenRatioByMoney","OpenRatioByVolume","CloseRatioByMoney","CloseRatioByVolume",
                "CloseTodayRatioByMoney","CloseTodayRatioByVolume",
            不填合约码则返回全部账户嵌套持仓字典,字典健为合约码,值为持仓字典
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        if InstrumentID in self.positions:
            return self.positions[InstrumentID]
        else:
            self.get_quote(InstrumentID)
            r = self._sendReq({"reqfuncname":"get_position","InstrumentID":InstrumentID,"wait_return":wait_return,"_ctp":_ctp, })
            if InstrumentID:
                self.positions[InstrumentID] = Position( )
                self.positions[InstrumentID].update(r['ret'])
                return self.positions[InstrumentID]
            else: 
                for s in r['ret']:
                    if s not in self.positions: self.positions[s] = Position( )
                    self.positions[s].update(r['ret'][s])
                return self.positions
    
    def get_quote(self,InstrumentID:str,wait_return=True,subscribe=True) -> Quote:
        '''
        Args:
            InstrumentID: 合约码
        Return:
            返回合约的行情Quote对象
        '''
        if not self.check_instrument(InstrumentID): return
        if InstrumentID not in self.instruments_property:
            self.get_symbol_info(InstrumentID)
        if InstrumentID in self.quote and InstrumentID not in self.unsubscribe_instruments:
            return self.quote[InstrumentID]
        elif InstrumentID:
            if subscribe: 
                q = self.subscribe_quote([InstrumentID]) #订阅行情
            r = self._sendReq({"reqfuncname":"get_quote","InstrumentID":InstrumentID,"wait_return":wait_return, })
            if not r['ret'] : 
                e = f"{datetime.now()} -get_quote查询行情失败\n"
                self.logs_txt(e,self._logfile,_print=True)
            else:
                while InstrumentID not in self.quote:  #等待行情更新
                    tm.sleep(0.001)
                else: return self.quote[InstrumentID]

    def get_symbol_trade(self,InstrumentID:str = "",wait_return=False) -> List[Trade]:
        '''
        获取合约成交单
        Ags:
            InstrumentID: 合约代码,如rb2601,不填则查询全部成交单
        Return:
            由合约的成交单Trade对象组成的列表
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_trade","InstrumentID":InstrumentID,"wait_return":wait_return})
        if not r['ret']: 
            e = f"{datetime.now()} -get_symbol_trade查询成交单为空\n"
            self.logs_txt(e,self._logfile,_print=True)
            return []
        return [Trade().update(t).update({"local_timestamp":tm.time()}) for t in r['ret']]

    def get_symbol_order(self,InstrumentID:str = "", OrderStatus:str = "",CombOffsetFlag:str="",wait_return=False) -> List[Order]:
        '''
        获取合约委托单
        Ags:
            InstrumentID: 合约代码,如rb2601,不填则查询全部委托单
            OrderStatus: 委托单状态,"Alive"查询活动委托单,"Finished"查询已完结委托单
            CombOffsetFlag: Open开仓单,Close平仓单
        Return:
            由委托单Order对象组成的列表
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_order","InstrumentID":InstrumentID,"OrderStatus":OrderStatus,"CombOffsetFlag":CombOffsetFlag,"wait_return":wait_return })
        if not r['ret']: 
            e = f"{datetime.now()} -get_symbol_order查询委托单为空\n"
            self.logs_txt(e,self._logfile,_print=True)
            return []
        return [Order().update(o).update({"local_timestamp":tm.time()}) for o in r['ret']]
            
    def get_id_order(self,InstrumentID:str="", order_id:str="",OrderMemo:str="",wait_return=False) -> Union[List[Order] , Order]:
        '''
        获取合约委托单
        Ags:
            InstrumentID: 合约代码,如rb2601
            order_id: 为委托单order编号组成的字符串:f'{order["FrontID"]}_{order["SessionID"]}_{order["OrderRef"]}'
        Return:
            order_id有效时返回委托单对象,order_id不设置时返回委托单对象组成的列表
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        if order_id in self.orders:
            return self.orders[order_id]
        else:
            r = self._sendReq({"reqfuncname":"get_id_order","InstrumentID":InstrumentID,"order_id":order_id,"OrderMemo":OrderMemo,"wait_return":wait_return, })
            if isinstance(r['ret'],dict): 
                self.orders[order_id] = Order( )
                self.orders[order_id].update(r['ret'])
                self.orders[order_id].update({"local_timestamp":tm.time()})
                return self.orders[order_id]
            elif isinstance(r['ret'],list):
                if not r['ret']: 
                    e = f"{datetime.now()} -get_id_order查询委托单为空\n"
                    self.logs_txt(e,self._logfile,_print=True)
                else: 
                    for o in r['ret']:
                        if o["order_id"] not in self.orders: 
                            self.orders[o["order_id"]] = Order( )
                            self.orders[o["order_id"]].update(o)
                            self.orders[o["order_id"]].update({"local_timestamp":tm.time()})
                    if order_id: return self.orders[order_id]
                    return [self.orders[o["order_id"]] for o in r['ret']]
                       
    def get_trade_of_order(self, order_id:str,wait_return=False,_print=True) -> List[Trade]:
        '''
        获取合约委托单对应的成交单
        Ags:
            order_id: 必填,委托单order_id
        Return:
            由成交单字典组成的列表
        '''
        r = self._sendReq({"reqfuncname":"get_trade_of_order","order_id":order_id,"wait_return":wait_return, })
        if not r['ret']: 
            if _print:
                e = f"{datetime.now()} -get_trade_of_order查询成交单失败\n"
                self.logs_txt(e,self._logfile,_print=_print)
            return []
        else:
            return [Trade().update(t).update({"local_timestamp":tm.time()}) for t in r['ret']]
    
    def get_order_risk(self,InstrumentID:str = "",wait_return=False) -> dict:
        '''
        获取合约委托单风控统计
        Ags:
            InstrumentID: 合约代码,如rb2601
        Return:
            字典,健为:
                "cancel_count","open_volume","order_count",
                "self_trade_count","order_exe","otr","repeat"
                报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量、报单成交比
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_order_risk","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: 
            e = f"{datetime.now()} -get_order_risk查询委托单风控失败\n"
            self.logs_txt(e,self._logfile,_print=True)
        return r['ret']

    def get_frozen_pos(self,InstrumentID:str,wait_return=False) -> dict:
        '''
        获取合约冻结仓位,上期所、能源中心区分昨仓、今仓,其他交易所只有昨仓冻结(体现在今仓冻结字段)
        由活动委托单计算得出
        Return:
            {"short_frozen_today":0,"long_frozen_today":0,
            "short_frozen_his":0,"long_frozen_his":0}
        '''
        if not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_frozen_pos","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: 
            e = f"{datetime.now()} -get_frozen_pos查询冻结持仓失败\n"
            self.logs_txt(e,self._logfile,_print=True)
        return r['ret']
    
    def get_frozen_margin(self,InstrumentID:str="",wait_return=False) -> float:
        '''
        获取冻结保证金,按多空活动开仓单保证金求和,默认多空开仓单都占用保证金,不考虑保证金优惠
        Args:
            InstrumentID: 合约代码,如rb2601,不填则查询账户冻结保证金
        Return:
            保证金之和
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_frozen_margin","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: 
            e = f"{datetime.now()} -get_frozen_margin查询冻结保证金失败\n"
            self.logs_txt(e,self._logfile,_print=True)
        return r['ret']
        
    def get_symbol_info(self,InstrumentID:str,wait_return=False) -> InstrumentProperty:
        '''
        获取单个具体合约(期货或期权)属性
        Args:
            InstrumentID:str,合约代码,若查询合约不存在返回空字典
        Return:
            合约属性对象InstrumentProperty
            若查询不存在返回None
        '''
        if not self.check_instrument(InstrumentID): return
        if InstrumentID in self.instruments_property:
            return self.instruments_property[InstrumentID]
        else:
            r = self._sendReq({"reqfuncname":"get_symbol_info","InstrumentID":InstrumentID,"wait_return":wait_return, })
            self.instruments_property[InstrumentID] = InstrumentProperty()
            self.instruments_property[InstrumentID].update(r['ret'])
            if self.instruments_property[InstrumentID]['OptionsType'] == '1': self.instruments_property[InstrumentID]['OptionsType'] = 'CALL'
            elif self.instruments_property[InstrumentID]['OptionsType'] == '2': self.instruments_property[InstrumentID]['OptionsType'] = 'PUT'
            return self.instruments_property[InstrumentID]
    def query_symbol(self,ProductID:str="",ExchangeID:str="",ProductClass:str="",OptionsType:str="") -> polars.DataFrame:
        '''
        根据交易所ID或品种ID查询合约属性,可查询期货、期权合约属性
        Args:
            ProductID:str,品种ID,如rb
            ExchangeID:str,交易所代码,上期所SHFE,大商所DCE,郑商所CZCE,能源中心INE,中金所CFFEX,广期所GFEX
                    不填则查找全部交易所
            ProductClass:str,查找合约类型,期货"Future",期权"Option"
            OptionsType:str,期权类型,看涨期权CALL,看跌期权PUT
        Return:
            polars.DataFrame 若查询不存在返回空表格
        '''
        r = self._sendReq({"reqfuncname":"query_symbol","ExchangeID":ExchangeID,"ProductID":ProductID,"ProductClass":ProductClass,
                            "OptionsType":OptionsType,})
        return r['ret']
    def query_symbol_option(self,UnderlyingInstrID:str,OptionsType:str) -> List[polars.DataFrame]:
        '''
        根据标的合约查询上市中的期权合约属性
        Args:
            UnderlyingInstrID:str,标的合约码
            OptionsType:str,期权类型,"CALL"看涨期权,"PUT"看跌期权
        Return:
            [ polars.DataFrame表格 , polars.DataFrame表格 ]
            返回不同到期日期权组成的列表,元素为polars.DataFrame表格
            若查询不存在返回空列表
            表格形式为:
            ┌───────────┬─────────────────┬────────────┬───────────────┬───┬─────────────┬───────────────────┬────────────────────┬────────────────┐
            │ row_index ┆ CombinationType ┆ CreateDate ┆ DeliveryMonth ┆ … ┆ StrikePrice ┆ UnderlyingInstrID ┆ UnderlyingMultiple ┆ VolumeMultiple │
            │ ---       ┆ ---             ┆ ---        ┆ ---           ┆   ┆ ---         ┆ ---               ┆ ---                ┆ ---            │
            │ u32       ┆ str             ┆ i64        ┆ i64           ┆   ┆ f64         ┆ str               ┆ f64                ┆ i64            │
            ╞═══════════╪═════════════════╪════════════╪═══════════════╪═══╪═════════════╪═══════════════════╪════════════════════╪════════════════╡
            │ 0         ┆ 0               ┆ 20250612   ┆ 10            ┆ … ┆ 5000.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 1         ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 5100.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 2         ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 5200.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 3         ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 5300.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 4         ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 5400.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ …         ┆ …               ┆ …          ┆ …             ┆ … ┆ …           ┆ …                 ┆ …                  ┆ …              │
            │ 9         ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 5900.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 10        ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 6000.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 11        ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 6100.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 12        ┆ 0               ┆ 20250605   ┆ 10            ┆ … ┆ 6200.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            │ 13        ┆ 0               ┆ 20250714   ┆ 10            ┆ … ┆ 6300.0      ┆ SR511             ┆ 1.0                ┆ 10             │
            └───────────┴─────────────────┴────────────┴───────────────┴───┴─────────────┴───────────────────┴────────────────────┴────────────────┘
        '''
        r = self._sendReq({"reqfuncname":"query_symbol_option","UnderlyingInstrID":UnderlyingInstrID,"OptionsType":OptionsType,})
        if not r['ret']: 
            e = f"{datetime.now()} -query_symbol_option查询期权为空:{r['ret']}\n"
            self.logs_txt(e,self._logfile,_print=True)
        return r['ret']
    def get_option(self,underlying_price,price_level,group_option:polars.DataFrame) -> dict:
        '''
        查询以价格underlying_price为基准的档位price_level对应的期权
        Args:
            underlying_price:float,标的价格
            price_level:int,期权档位,正值实值期权,负值虚值期权,0平值期权
            group_option: 由query_symbol_option返回的期权表格
        Return:
            {"option":SR511P5500,"strike_price":5500.0,"spread":100.0}
        '''
        r = self._sendReq({"reqfuncname":"get_option","underlying_price":underlying_price,
                            "price_level":price_level,"group_option":group_option,})
        return r['ret']

    def get_symbol_marginrate(self,InstrumentID:str,wait_return=False) -> dict:
        '''
        获取合约保证金率(非期权)
        Args:
            InstrumentID: 合约代码,如rb2601
        Return:
            若查询不到返回缺省值0
            保证金率字典,包含健 "LongMarginRatioByMoney", "ShortMarginRatioByMoney",
            "LongMarginRatioByVolume", "ShortMarginRatioByVolume",
            "LongMarginRatio","ShortMarginRatio"
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_marginrate","InstrumentID":InstrumentID,"wait_return":wait_return, })
        return r['ret']

    def get_symbol_commission(self,InstrumentID:str,wait_return=False) -> dict:
        '''
        获取合约手续费率(非期权)
        Args:
            InstrumentID: 合约代码,如rb2601
        Return:
            若查询不到返回缺省值0
            手续费率字典,包含健 "OpenRatioByMoney","OpenRatioByVolume","CloseRatioByMoney",
            "CloseRatioByVolume","CloseTodayRatioByMoney","CloseTodayRatioByVolume",
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_commission","InstrumentID":InstrumentID,"wait_return":wait_return, })
        return r['ret']
    
    def get_trade_commission(self,InstrumentID:str,TradeID:str="",wait_return=False) -> float:
        '''
        Args:
            InstrumentID: 合约代码,如rb2601
            TradeID: 成交单编号,若不填则计算合约全部成交单手续费
        Return:
            float,手续费总和
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        if TradeID and TradeID not in self.trades: return 
        r = self._sendReq({"reqfuncname":"get_trade_commission","InstrumentID":InstrumentID,"TradeID":TradeID,"wait_return":wait_return, })
        #if not r['ret']: print("查询成交单手续费失败")
        return r['ret']

    def get_option_commission(self,InstrumentID:str="", wait_return=False):
        '''
        获取期权合约手续费率
        Args:
            InstrumentID: 合约代码,如rb2601-P-3500
        Return:
            若查询不到返回缺省值0
            手续费率字典,包含健 
            "OpenRatioByMoney","OpenRatioByVolume","CloseRatioByMoney","CloseRatioByVolume",
            "CloseTodayRatioByMoney","CloseTodayRatioByVolume","StrikeRatioByMoney","StrikeRatioByVolume":0,
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_option_commission","InstrumentID":InstrumentID, "wait_return":wait_return, })
        return r['ret']

    def get_option_marginrate(self,InstrumentID:str,InputPrice,UnderlyingPrice=0,HedgeFlag='1',wait_return=False) -> dict:
        '''
        获取期权合约保证金率 
        #保证金=max(权利金+FixedMargin,MiniMargin)
        #每手 期权卖方交易保证金 = 权利金 + max(标的期货合约保证金 - 期权虚值额的一半，标的期货合约保证金的一半)
        #max(标的期货合约保证金 - 期权虚值额的一半 , 标的期货合约保证金的一半)为不变量,均以结算价和行权价计算
        Args:
            InstrumentID: 合约代码,如rb2601P3500
            InputPrice: 期权合约报价
            UnderlyingPrice： 标的合约价格,默认昨结算价
        Return:
            若查询不到返回缺省值0
            保证金率字典,包含健 
            "FixedMargin","MiniMargin","Royalty","ExchFixedMargin","ExchMiniMargin","LongMarginRatioByMoney",
            "ShortMarginRatioByMoney","LongMarginRatioByVolume","ShortMarginRatioByVolume","LongMarginRatio",
            "ShortMarginRatio",
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_option_marginrate","InstrumentID":InstrumentID,"InputPrice":InputPrice,"UnderlyingPrice":UnderlyingPrice,"HedgeFlag":HedgeFlag,"wait_return":wait_return, })
        return r['ret']
    
    def get_option_magin(self,InstrumentID,InputPrice,UnderlyingPrice=0,HedgeFlag='1',wait_return=False):
        '''
        计算期权保证金,变化的只有权利金,由InputPrice计算,当未订阅行情时,默认以行权价计算
        只是估计计算，因为其使用的公式()保证金=max(权利金+FixedMargin,MiniMargin))中的权利金部分在计算时使用的期权价格是InputPrice。
        而资金查询里的期权保证金计算公式中的期权价格是使用max算法(max(昨结算，最新价))得到的
        Args:
            InstrumentID: 合约代码,如rb2601P3500
            InputPrice: 期权合约报价
            UnderlyingPrice： 标的合约价格,默认昨结算价
        Return:
            float,保证金
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_option_magin","InstrumentID":InstrumentID,"InputPrice":InputPrice,"UnderlyingPrice":UnderlyingPrice,"HedgeFlag":HedgeFlag,"wait_return":wait_return, })
        return r['ret']

    def _wait_order(self,ReqID:int):
        try:
            r = self._ans_queue.get_nowait()
            if ReqID in r: return r[ReqID] #查询完成
            else: self._ans_queue.put_nowait(r)
        except queue.Empty:
            return True
 
    def cancel_order(self, order_id:str,OrderMemo="",wait_return=False):
        '''撤单阻塞函数,等待撤单结束'''
        if order_id not in self.orders:
            print("委托单号不存在")
            return
        if order_id in self.orders and self.orders[order_id]["OrderStatus"] in ['0','2','4' ,'5' ]: return
        r = self._sendReq({"reqfuncname":"cancel_order","order_id":order_id,"OrderMemo":OrderMemo,"wait_return":wait_return,})
        if not r['ret'] or isinstance(r['ret'],str) :
            e = f"{datetime.now()} -cancel_order撤单失败,order_id:{order_id},错误信息:{r['ret']}\n"
            self.logs_txt(e,self._logfile,_print=True)
            return
        else: #撤单发出但不代表被交易所接受
            e = f"{datetime.now()} -cancel_order已撤单,order_id:{order_id}\n"
            self.logs_txt(e,self._logfile,_print=True)
            return

    def cancel_all_order(self,InstrumentID:str="",OrderMemo: str = "",wait_return=False):
        '''批量撤单,阻塞函数,等待撤单结束'''
        orders = []
        if InstrumentID :
            if not self.check_instrument(InstrumentID): return
            orders = self.get_symbol_order(InstrumentID,"Alive")
        else: orders = self.get_symbol_order(OrderStatus = "Alive")
        if orders:
            for o in orders:
                self.cancel_order(o["order_id"],OrderMemo=OrderMemo)
        
    def insert_order(self,ExchangeID:str,InstrumentID:str,Direction:str,Offset:str,Volume:int,LimitPrice:float,advanced=None,HedgeFlag:str="1",WaitReturn=False,OrderMemo="") -> Union[None,Order]:
        '''非阻塞函数,报单被交易所接受后返回字典'''
        if not self.check_instrument(InstrumentID): return
        quote = self.get_quote(InstrumentID)
        instrument_property = self.get_symbol_info(InstrumentID)
        exchange_id = instrument_property["ExchangeID"]
        PriceTick = instrument_property["PriceTick"]
        #VolumeMultiple = instrument_property["VolumeMultiple"]
        if (not isinstance(LimitPrice,float) or LimitPrice != LimitPrice or not isinstance(Volume,int) or LimitPrice/PriceTick - int(LimitPrice/PriceTick) != 0 or
            Volume <= 0 or Volume < instrument_property["MinLimitOrderVolume"] or Volume > instrument_property["MaxLimitOrderVolume"] 
            or Direction not in ["Buy","Sell"] or Offset not in ["Open","Close","CloseToday"] or ExchangeID != exchange_id
            or (LimitPrice > quote["UpperLimitPrice"] and Direction == "Buy" or LimitPrice < quote["LowerLimitPrice"] and Direction == "Sell")): 
            e = ("{},{},{}\n".format(
                f"{datetime.now()} -insert_order报单失败,输入参数不合法,价格:{LimitPrice},数量:{Volume},最小限价单数量:{instrument_property['MinLimitOrderVolume']}", 
                f"最大限价单数量:{instrument_property['MaxLimitOrderVolume']},跌停价:{quote['LowerLimitPrice']},涨停价:{quote['UpperLimitPrice']}",
                f"最小跳:{PriceTick},交易方向:{Direction},开平:{Offset},合约交易所:{exchange_id}"
            ))
            self.logs_txt(e,self._logfile,_print=True)
            return e
        r = self._sendReq({"reqfuncname":"insert_order","ExchangeID":ExchangeID,"InstrumentID":InstrumentID,"Direction":Direction,"Offset":Offset,"Volume":Volume,"LimitPrice":LimitPrice,
                            "advanced":advanced,"HedgeFlag":HedgeFlag,"WaitReturn":WaitReturn,"OrderMemo":OrderMemo})
        if not isinstance(r['ret'],dict) :
            e = f"{datetime.now()} -insert_order报单失败,客户端拒绝或报单错误,错误信息:{r['ret']}\n"
            self.logs_txt(e,self._logfile,_print=True)
            return r['ret']
        else:  #{order_id:order}
            for order_id in r['ret']:
                if order_id not in self.orders: 
                    self.orders[order_id] = Order( )
                self.orders[order_id].update(r['ret'][order_id])
                self.orders[order_id].update({"local_timestamp":tm.time()})
            #报单发出成功
            return self.orders[order_id]

    def open_close(self,symbol,kaiping='',lot=0,price=None,block=True,n_price_tick=1,che_time=0,order_info='无',signal_price=float('nan'),order_close_chan=True,HedgeFlag:str="1",WaitReturn=False,OrderMemo="",**kw):
        '''
        报单,只支持限价下单,市价单以停板价报单,实际效果等效于市价单(部分交易所不支持市价单)
        symbol: str,合约代码
        kaiping: str,开平,kaiduo,kaikong,pingduo,pingkong,汉语拼音,表示 开多 开空 平多 平空
        lot: int,下单手数
        price:float,下单价格
        block:True,阻塞等待委托单结束
        n_price_tick: 排队价格偏离报单价多少跳不成交撤单,0不按价格偏离撤单
        che_time: 多少时间不成交则撤单,默认0不按时间撤单
        order_info:报单备注,例如是止损、止盈、策略1触发等
        signal_price:信号触发的价格位置
        order_close_chan:True,统计平仓单的盈亏
        '''
        if not self.check_instrument(symbol): 
            return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price, "last_msg":f"合约码不存在,合约码:{symbol}" ,
                    "quote_volume":float('nan'),"order_id":[],"trades":[],"order_wrong":True,"signal_price":signal_price,"order_info":order_info,"quote":{},"position":{}}
        night_time = kw["night_time"] if "night_time" in kw else None  #夜盘收盘时间
        close_minutes = kw["close_minutes"] if "close_minutes" in kw else 0 #临近夜盘收盘多少分钟停止交易并撤单
        open_min_volume = kw['open_min_volume'] if 'open_min_volume' in kw else 1 #开仓最小手数
        order_wrong = False #是否错单
        quote = self.get_quote(symbol)
        #symbol = quote["InstrumentID"] #合约代码  
        #ExchangeID = quote["ExchangeID"] #交易所代码
        position = self.get_position(symbol)
        instrument_property = self.get_symbol_info(symbol)
        ExchangeID = instrument_property["ExchangeID"]
        PriceTick = instrument_property["PriceTick"]
        VolumeMultiple = instrument_property["VolumeMultiple"]
        ctp_time = (quote["ctp_datetime"]+timedelta(minutes=close_minutes)).time()
        equal_time =  quote["ctp_datetime"].hour == datetime.now().hour #行情和本地时间同步确保夜盘品种在白盘已更新
        trading_night_time = isinstance(night_time,time) and close_minutes > 0
        if trading_night_time : #只监控夜盘,白盘跨15点会结算
            if ctp_time >= night_time > time(20) or time(8) > ctp_time >= night_time :
                e = f"{datetime.now()} -open_close临近收盘停止交易,收盘时间:{night_time}\n"
                self.logs_txt(e,self._logfile,_print=False)
                return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,
                    "last_msg":f"临近收盘停止交易" ,
                    "order_id":[],"order_wrong":True if equal_time else False,"signal_price":signal_price,"order_info":order_info,
                    "quote":quote,"position":position}
        advanced = None
        quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"] #盘口挂单量
        if not price or price == '对手价':
            if quote["AskPrice1"] <= quote["UpperLimitPrice"]:
                price_buy = quote["AskPrice1"]
            else: price_buy = quote["LastPrice"] #停板时以最新价报单
            if quote["BidPrice1"] >= quote["LowerLimitPrice"]:
                price_sell = quote["BidPrice1"]
            else: price_sell = quote["LastPrice"]
        elif price == '停板价':
            price_buy = quote["UpperLimitPrice"]
            price_sell = quote["LowerLimitPrice"]
        elif price == '排队价':
            if quote["BidPrice1"] >= quote["LowerLimitPrice"]:
                price_buy = quote["BidPrice1"]
            else: price_buy = quote["LastPrice"] #停板时以最新价报单
            if quote["AskPrice1"] <= quote["UpperLimitPrice"]:
                price_sell = quote["AskPrice1"]
            else: price_sell = quote["LastPrice"]
            advanced = None
        elif price == price :
            if price >= quote["UpperLimitPrice"] :
                price_buy = price_sell = quote["UpperLimitPrice"] #超出停板时以停板价报单
            elif price <= quote["LowerLimitPrice"]:
                price_buy = price_sell = quote["LowerLimitPrice"] #超出停板时以停板价报单
            else: price_buy = price_sell = price #其他限定价
            if price_buy < quote["AskPrice1"] and kaiping in ["kaiduo","pingkong"]:advanced = None
            if price_sell > quote["BidPrice1"] and kaiping in ["pingduo","kaikong"]:advanced = None
        if price != price or not isinstance(lot,int) or n_price_tick < 0 or che_time < 0:
            e = f"{datetime.now()} -open_close下单价格或数量或撤单跳数或撤单时间不合法,下单价格:{price},下单手数:{lot},撤单跳数:{n_price_tick},撤单时间:{che_time}\n"
            self.logs_txt(e,self._logfile,_print=False)
            return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price,
                    "last_msg":f"下单价格或数量或撤单跳数或撤单时间不合法,下单价格:{price},下单手数:{lot},撤单跳数:{n_price_tick},撤单时间:{che_time}" ,
                    "quote_volume":quote_volume,"order_id":[],"trades":[],"order_wrong":True,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
        lot = int(lot)
        lot = min(lot, instrument_property["MaxLimitOrderVolume"])
        if (price_buy/PriceTick - int(price_buy/PriceTick) != 0 or price_sell/PriceTick - int(price_sell/PriceTick) != 0 or
            lot <= 0 or lot < instrument_property["MinLimitOrderVolume"] or lot < open_min_volume): 
            #以持仓数量平仓时平仓数量为0可能是服务器故障持仓未更新(也可能其他程序超额平仓,或清仓代码正常所需非本策略bug),等待更新后可继续下单,其他情况下的报价和手数错误应退出交易
            if not (lot == position["pos_long"] == 0 and kaiping == 'pingduo' or lot == position["pos_short"] == 0 and kaiping=='pingkong'): 
                order_wrong = True 
            e = f"{datetime.now()} -open_close报单错误,下单价格:{price},下单手数:{lot},最大限价单:{instrument_property['MaxLimitOrderVolume']},最小限价单:{instrument_property['MinLimitOrderVolume']},最小开仓单:{open_min_volume},价格最小跳:{PriceTick}\n"
            self.logs_txt(e,self._logfile,_print=False)
            return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                    "last_msg":f"报单错误,下单价格:{price},下单手数:{lot},最大限价单:{instrument_property['MaxLimitOrderVolume']},最小限价单:{instrument_property['MinLimitOrderVolume']},最小开仓单:{open_min_volume},价格最小跳:{PriceTick}" ,
                    "quote_volume":quote_volume,"order_id":[],"trades":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
    
        shoushu = 0 #已成交手数  
        junjia = 0.0 #成交均价 
        che_count, day_order = 0, 0 #报撤单次数
        ping_jin,ping_zuo,order = None,None,None  #委托单对象
        last_msg, order_id, trades = "", [], []  #委托单状态信息和单号
        if kaiping== 'pingduo': #交易方向为平多
            #可能服务器故障持仓未更新,等待更新
            if not position["pos_long"] or position["open_price_long"] != position["open_price_long"]:
                e = f"{datetime.now()} -open_close多头持仓错误,多头持仓手数{position['pos_long']},多头持仓价格{position['open_price_long']}\n"
                self.logs_txt(e,self._logfile,_print=False)
                return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                        "last_msg":f"多头持仓错误,多头持仓手数{position['pos_long']},多头持仓价格{position['open_price_long']}" ,
                        "quote_volume":quote_volume, "order_id":[],"trades":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
            if 0 < lot <= position["pos_long_today"] : #小于等于今仓，平今
                ping_jin=self.insert_order(ExchangeID,symbol,'Sell','CloseToday', lot, price_sell,OrderMemo=OrderMemo)
                if isinstance(ping_jin,str): last_msg += ping_jin
            elif 0 < position["pos_long_today"] < lot <= position["pos_long"]:  #优先平今,剩余仓位平昨   
                ping_zuo=self.insert_order(ExchangeID,symbol,'Sell','Close',lot-position["pos_long_today"],price_sell,OrderMemo=OrderMemo) #先平昨再平今 
                ping_jin=self.insert_order(ExchangeID,symbol,'Sell','CloseToday',position["pos_long_today"],price_sell,OrderMemo=OrderMemo)
                if isinstance(ping_zuo,str): last_msg += ping_zuo
                if isinstance(ping_jin,str): last_msg += ping_jin
            elif 0 == position["pos_long_today"] < lot <= position["pos_long"]: #只有昨仓
                ping_zuo=self.insert_order(ExchangeID,symbol,'Sell','Close',lot,price_sell,OrderMemo=OrderMemo) 
                if isinstance(ping_zuo,str): last_msg += ping_zuo
            else: last_msg += "平仓数量不足"
        elif kaiping=='pingkong': #交易方向为平空
            if not position["pos_short"] or position["open_price_short"] != position["open_price_short"]:
                e = f"{datetime.now()} -open_close空头持仓错误,空头持仓手数{position['pos_short']},空头持仓价格{position['open_price_short']}\n"
                self.logs_txt(e,self._logfile,_print=False)
                return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                        "last_msg":f'空头持仓错误,空头持仓手数{position["pos_short"]},空头持仓价格{position["open_price_short"]}' ,
                        "quote_volume":quote_volume, "order_id":[],"trades":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
            if 0 < lot <= position["pos_short_today"] : #小于等于今仓，平今
                ping_jin=self.insert_order(ExchangeID,symbol,'Buy','CloseToday',lot,price_buy,OrderMemo=OrderMemo)    
                if isinstance(ping_jin,str): last_msg += ping_jin
            elif 0 < position["pos_short_today"] < lot <= position["pos_short"]:      
                ping_zuo=self.insert_order(ExchangeID,symbol,'Buy','Close',lot-position["pos_short_today"],price_buy,OrderMemo=OrderMemo)      
                ping_jin=self.insert_order(ExchangeID,symbol,'Buy','CloseToday',position["pos_short_today"],price_buy,OrderMemo=OrderMemo)
                if isinstance(ping_zuo,str): last_msg += ping_zuo
                if isinstance(ping_jin,str): last_msg += ping_jin
            elif 0 == position["pos_short_today"] < lot <= position["pos_short"]:      
                ping_zuo=self.insert_order(ExchangeID,symbol,'Buy','Close',lot,price_buy,OrderMemo=OrderMemo)
                if isinstance(ping_zuo,str): last_msg += ping_zuo
            else: last_msg += "平仓数量不足"
                
        elif kaiping== 'kaiduo': #交易方向为开多
            order = self.insert_order(ExchangeID,symbol,'Buy','Open',lot,price_buy,OrderMemo=OrderMemo)   
            if isinstance(order,str): last_msg += order
        elif kaiping=='kaikong': #交易方向为开空
            order = self.insert_order(ExchangeID,symbol,"Sell","Open",lot,price_sell,OrderMemo=OrderMemo)
            if isinstance(order,str): last_msg += order
        else: last_msg += f"交易方向{kaiping}只支持:kaiduo、kaikong、pingduo、pingkong"
        t = datetime.now().timestamp() #时间起点
        finished = ['0','4' ,'5' ]
        update_time = []
        local_timestamp = quote["local_timestamp"]
        if isinstance(ping_zuo,Order): #报单成功发向交易所
            orderid = ping_zuo["order_id"]
            order_id.append(orderid)
            last_price = quote.LastPrice
            while True:
                trades_list = self.get_trade_of_order(orderid,_print=False)
                trades.extend(trades_list)
                ping_zuo = self.get_id_order(order_id=orderid) #收到成交回报后再查询一次委托单更新,避免已全部成交的单触发误撤
                quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                ctp_time = (quote["ctp_datetime"]+timedelta(minutes=close_minutes)).time()
                if ping_zuo["OrderStatus"] not in finished or ping_zuo["VolumeTraded"] != sum( trade["Volume"] for trade in trades_list): #平昨单是否完成
                    if not advanced and not block :break #当日有效单，且无需等待是否完成
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if ping_zuo["OrderStatus"] not in finished and not advanced and (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= ping_zuo["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= ping_zuo["LimitPrice"]-PriceTick*n_price_tick)) 
                    and last_price != quote.LastPrice == quote.LastPrice 
                        or (trading_night_time and (ctp_time >= night_time > time(20) or time(8) > ctp_time >= night_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                        last_price = quote.LastPrice
                else: break
            day_order += 1 #报单数加1
            volume = ping_zuo["VolumeTraded"] #成交手数
            if ping_zuo["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += ping_zuo["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]
                    update_time.append(trade["local_timestamp"])

        if isinstance(ping_jin,Order): #报单成功发向交易所
            orderid = ping_jin["order_id"]
            order_id.append(orderid)
            last_price = quote.LastPrice
            while True:
                trades_list = self.get_trade_of_order(orderid,_print=False)
                trades.extend(trades_list)
                ping_jin = self.get_id_order(order_id=orderid) #收到成交回报后再查询一次委托单更新,避免已全部成交的单触发误撤
                quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                ctp_time = (quote["ctp_datetime"]+timedelta(minutes=close_minutes)).time()
                if ping_jin["OrderStatus"] not in finished or ping_jin["VolumeTraded"] != sum( trade["Volume"] for trade in trades_list): #平昨单是否完成
                    if not advanced and not block :break #当日有效单，且无需等待是否完成
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if ping_jin["OrderStatus"] not in finished and not advanced and (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= ping_jin["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= ping_jin["LimitPrice"]-PriceTick*n_price_tick)) 
                    and last_price != quote.LastPrice == quote.LastPrice 
                        or (trading_night_time and (ctp_time >= night_time > time(20) or time(8) > ctp_time >= night_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                        last_price = quote.LastPrice
                else: break
            day_order += 1 #报单数加1
            volume = ping_jin["VolumeTraded"] #成交手数
            if ping_jin["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += ping_jin["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]
                    update_time.append(trade["local_timestamp"])

        if isinstance(order,Order): #报单成功发向交易所
            orderid = order["order_id"]
            order_id.append(orderid)
            last_price = quote.LastPrice
            while True:
                trades_list = self.get_trade_of_order(orderid,_print=False)
                trades.extend(trades_list)
                order = self.get_id_order(order_id=orderid) #收到成交回报后再查询一次委托单更新,避免已全部成交的单触发误撤
                quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                ctp_time = (quote["ctp_datetime"]+timedelta(minutes=close_minutes)).time()
                if order["OrderStatus"] not in finished or order["VolumeTraded"] != sum( trade["Volume"] for trade in trades_list): #平昨单是否完成
                    if  not block :break #当日有效单，且无需等待是否完成
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if order["OrderStatus"] not in finished and  (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= order["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= order["LimitPrice"]-PriceTick*n_price_tick)) 
                    and last_price != quote.LastPrice == quote.LastPrice 
                        or (trading_night_time and (ctp_time >= night_time > time(20) or time(8) > ctp_time >= night_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                        last_price = quote.LastPrice
                else: break
            day_order += 1 #报单数加1
            volume = order["VolumeTraded"] #成交手数
            if order["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += order["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]
                    update_time.append(trade["local_timestamp"])
        if shoushu: 
            junjia = junjia/shoushu #计算成交均价
            #junjia = round(junjia/shoushu,quote.price_decs) #保留和报价同样小数位
            for trade in trades_list:
                trade_id = f'{trade["ExchangeID"]}_{trade["OrderSysID"]}_{trade["TradeID"]}'
                while trade_id not in self.trades: tm.sleep(0.001)
                
        else: #错单原因
            junjia = float('nan')
            #集合竞价,未到开盘时间等待60秒
            if "拒绝" in last_msg and ("竞价" in last_msg or "竟价" in last_msg or "交易时间" in last_msg): tm.sleep(60)
            elif ("拒绝" in last_msg or "限制" in last_msg or "不足" in last_msg or "超过" in last_msg or "不支持" in last_msg or "低于" in last_msg
                  or "权限" in last_msg or "平仓" in last_msg or "开仓" in last_msg or "禁止" in last_msg or "状态" in last_msg or "操作" in last_msg
                  or "交易" in last_msg or "产品" in last_msg or "CTP" in last_msg or "开户" in last_msg or "不允许" in last_msg or "报单" in last_msg
                   or "交易所" in last_msg or "合约" in last_msg or "确认" in last_msg or "错误" in last_msg or "有误" in last_msg):
                order_wrong = True
        e = f"{datetime.now()} -open_close下单完成,合约:{symbol},交易方向{kaiping},成交手数:{shoushu},报单价:{price},成交均价:{junjia},委托单信息:{last_msg},备注:{order_info}\n"
        self.logs_txt(e,self._logfile,_print=False)    
        return {"shoushu":shoushu, "junjia":junjia, "che_count":che_count, "day_order":day_order, "symbol":symbol, "kaiping":kaiping, "lot":lot, "price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell, "last_msg":last_msg, 
                "quote_volume":quote_volume, "order_id":order_id,"trades":trades,"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position} #返回成交手数、成交均价,和主动撤单次数，若无成交则均价为nan值
        
    def _rtn_thread(self):
        for i in self._rtn_queue:
            if i == "exception":  
                return
            if isinstance(i,dict):
                t = tm.time()
                if "instruments" in i:
                    self.instruments = i["instruments"] #全部合约码
                elif "pos" in i:
                    for s in i["pos"]:
                        if s not in self.positions: self.positions[s] = Position( )
                        self.positions[s].update(i["pos"][s])
                        self.positions[s].update({"local_timestamp":t})
                elif "quote" in i:
                    InstrumentID = i["quote"]["InstrumentID"]
                    if InstrumentID in self.unsubscribe_instruments: continue
                    if InstrumentID not in self.quote: 
                        self.quote[InstrumentID] = Quote( )
                    #if self.quote[InstrumentID]["TradingDay"] != self.TradingDay: self.quote[InstrumentID]["TradingDay"] = self.TradingDay
                    #try:
                        # 确保毫秒部分为3位数
                    #    full_time_str = f'{i["quote"]["ActionDay"]} {i["quote"]["UpdateTime"]}.{i["quote"]["UpdateMillisec"]:03d}'
                    #    tick_time = datetime.strptime(full_time_str, "%Y%m%d %H:%M:%S.%f")
                    #except ValueError:
                    #    tick_time = datetime.strptime(f'{i["quote"]["ActionDay"]} {i["quote"]["UpdateTime"]}.{i["quote"]["UpdateMillisec"]}')
                    if i["quote"]["ActionDay"]:
                        dt = datetime.strptime(f'{i["quote"]["ActionDay"]} {i["quote"]["UpdateTime"]}.{i["quote"]["UpdateMillisec"]:03d}', "%Y%m%d %H:%M:%S.%f")
                    else: dt = datetime.now()
                    if dt == self.quote[InstrumentID].ctp_datetime: dt += timedelta(milliseconds=500) #郑商所毫秒值均为0
                    dt_timestamp = dt.timestamp()
                    trading_day_date = datetime.strptime(i["quote"]["TradingDay"], "%Y%m%d").date()
                    self.quote[InstrumentID].update(i["quote"])
                    self.quote[i["quote"]["InstrumentID"]].update({"local_timestamp":t,"ctp_timestamp":dt_timestamp,"ctp_datetime":dt,"trading_day":trading_day_date})
                    if self._save_tick:
                        if i["quote"]["InstrumentID"] in self._save_tick_symbols:
                            self._save_tick_queue.put(self.quote[i["quote"]["InstrumentID"]].to_dict())
                    #if "notify" not in self.quote[i["quote"]["InstrumentID"]]: self.quote[i["quote"]["InstrumentID"]]["notify"] = zhuchannel.ThreadChan(last_only=True)
                    #self.quote[i["quote"]["InstrumentID"]]["notify"].send(True)
                elif "account" in i:
                    self.account.update(i["account"])
                    self.account.update({"local_timestamp":t})
                elif "order" in i:
                    for order_id in i["order"]:
                        if order_id not in self.orders: self.orders[order_id] = Order( )
                        self.orders[order_id].update(i["order"][order_id]) 
                        self.orders[order_id].update({"local_timestamp":t})
                elif "trade" in i:
                    for trade_id in i["trade"]:
                        if trade_id not in self.trades: self.trades[trade_id] = Trade( )
                        self.trades[trade_id].update(i["trade"][trade_id]) 
                        self.trades[trade_id].update({"local_timestamp":t})
                elif "orderaction" in i:
                    for order_id in i["orderaction"]:
                        if order_id not in self.orders: self.orders[order_id] = Order( )
                        self.orders[order_id].update(i["order"][order_id]) 
                        self.orders[order_id].update({"local_timestamp":t})
    def _ans_thread(self):
        for i in self._ans_queue:
            if not isinstance(i,dict): 
                print('CTP连接中断,重新登录')
                continue
            if "instruments" in i:
                self.instruments = i["instruments"]
            elif "pos" in i:
                t = tm.time()
                for s in i["pos"]:
                    if s not in self.positions: self.positions[s] = Position(**i["pos"][s])
                    else: self.positions[s].update(i["pos"][s])
                    self.positions[s].update({"local_timestamp":t})
    def check_instrument(self,InstrumentID:str):
        if InstrumentID in self.instruments: return True
        else: 
            e = f"{datetime.now()} -合约{InstrumentID}不存在\n"
            self.logs_txt(e,self._logfile,_print=True)
            #raise Exception(f"合约{InstrumentID}不存在")
            return False
     
    def _sendReq(self,kw):
        '''
        不需要向ctp查询的直接收到的是结果,需要向ctp查询的首先收到的是发送成败
        r[ReqID]True和False表示向CTP发送成功或失败,r["return"]表示r[ReqID]为最终结果
        '''
        #等待CTP准备就绪
        while not self._Reqqueue.empty(): tm.sleep(0.000000001) 
        self._ReqID += 1
        ReqID = self._ReqID
        self._Reqqueue.put({**kw,"ReqID":ReqID})
        while True:
            r = self._ans_queue.get()
            if not isinstance(r,dict): continue  #断线重连,非数据反馈
            if ReqID in r and "return" in r and r["return"]: break #查询完成,得到最终结果
            elif ReqID not in r: self._ans_queue.put_nowait(r) #非本次查询
        return {"ReqID":ReqID,"ret":r[ReqID]}
    
    def subscribe_quote(self,InstrumentID:str,wait_return=False):
        '''只负责行情订阅,最新行情从quote中取'''
        if isinstance(InstrumentID,str): instrument_ids = [InstrumentID]
        elif isinstance(InstrumentID,list): instrument_ids = InstrumentID
        for s in instrument_ids:
            if not self.check_instrument(s): return
            if not s: 
                e = f"{datetime.now()} -合约码为空,订阅行情失败\n"
                self.logs_txt(e,self._logfile,_print=True)
                return
        #for s in InstrumentID:
        #    self.reqry_market_data(s)
        '''等待行情返回'''
        ss = [s for s in InstrumentID if s not in self.quote]
        if ss: 
            self._Reqqueue.put_nowait({"Subscribe":ss})
            for s in ss:
                if s in self.unsubscribe_instruments: self.unsubscribe_instruments.remove(s)
        
    def unsubscribe_quote(self,InstrumentID:str,wait_return=False):
        if isinstance(InstrumentID,str): instrument_ids = [InstrumentID]
        elif isinstance(InstrumentID,list): instrument_ids = InstrumentID
        for s in instrument_ids:
            if not self.check_instrument(s): return
            if not s: 
                e = f"{datetime.now()} -合约码为空,退订行情失败\n"
                self.logs_txt(e,self._logfile,_print=True)
                return
        ss = [s for s in instrument_ids if s in self.quote and s not in self.positions ] #有行情且无持仓才能退订
        if ss:
            self._Reqqueue.put_nowait({"UnSubscribe":ss})
            for s in ss: 
                if s in self.quote: self.quote.pop(s)
                if s not in self.unsubscribe_instruments: self.unsubscribe_instruments.add(s)
                    

    def reqry_market_data(self,InstrumentID:str,wait_return=False):
        return self.get_quote(InstrumentID,wait_return,subscribe=False)

    def get_kline(self,InstrumentID: str, period: str, kline_count:int, save_instrument_id: str = "") -> Optional[MutableKlineHolder]:
        '''
        Args:
            InstrumentID: 合约代码
            period: 周期,单位为s、m、h、d、w、M、y,表示秒、分、时、日、周、月、年,一周表示7天,一月表示30天,一年表示365天
            kline_count: k线数量,当本地tick数据不足以生成kline_count数量的K线时,则只返回可生成的数量,并在后续tick推送中逐渐更新到kline_count数量
            save_instrument_id:保存tick数据的名称,默认为合约码
        Return:
            返回MutableKlineHolder对象,通过data属性取值为polars.DataFrame
            包含基础列"InstrumentID","open","high","low","close","Volume","Turnover","OpenInterest"以及Quote对象字段
            "period_start" K线开始时间点, "period_end" K线结束时间点
            "period" K线周期
        用法:
            kline = get_kline("rb2110","1d",10)
            print(kline.data) #打印全部K线数据
            print(kline.tail(10)) #打印最新10根K线数据
        '''
        if not self.check_instrument(InstrumentID): return
        if InstrumentID not in self.quote: self.get_quote(InstrumentID)
        self._save_tick = True
        self._save_tick_symbols[InstrumentID] = save_instrument_id
        kline = self.CTP_data_processor.get_kline_holder(InstrumentID , period , kline_count)
        return kline
    
    def get_tick(self,InstrumentID: str, save_instrument_id: str = "") -> Optional[MutableTickHolder]:
        '''
        Args:
            InstrumentID: 合约代码
            save_instrument_id:保存tick数据的名称,默认为合约码。如果保存主连数据,则所有主力合约(01/05/09)都存储相同的名称
        Return:
            返回MutableTickHolder对象,通过data属性取值为polars.DataFrame
            包含列参见Quote对象字段
            
        用法:
            tick = get_tick("rb2110")
            print(tick.data) #打印全部Tick数据
            print(tick.tail(10)) #打印最新10根Tick数据
        '''
        if not self.check_instrument(InstrumentID): return
        if InstrumentID not in self.quote: self.get_quote(InstrumentID)
        self._save_tick = True
        self._save_tick_symbols[InstrumentID] = save_instrument_id
        tick = self.CTP_data_processor.get_tick_holder(InstrumentID )
        return tick

    def _update_tick_kline(self):
        for i in self._save_tick_queue:
            if i == "exception":  
                return
            self.CTP_data_processor.process_snapshot(snapshot = i, save_instrument_id = self._save_tick_symbols[i["InstrumentID"]])

    async def OpenClose(self,symbol:str, kaiping: str = '', lot: int = 0, price: float = None,block: bool = True, 
                        n_price_tick: int = 1, che_time: int = 0, order_info: str = '无', signal_price: float = float('nan'),
                        order_close_chan: bool = True, HedgeFlag: str = "1", WaitReturn: bool = False,OrderMemo="",**kw):
        '''open_close的协程版'''
        # 关键：用partial绑定open_close的关键字参数
        bound_func = partial(self.open_close, symbol=symbol, kaiping = kaiping, lot = lot, price = price, block = block,
                            n_price_tick = n_price_tick, che_time = che_time, order_info = order_info, signal_price = signal_price,
                            order_close_chan = order_close_chan, HedgeFlag = HedgeFlag, WaitReturn = WaitReturn, OrderMemo = OrderMemo,**kw)  # 先把参数绑定到函数上
        
        loop = asyncio.get_running_loop()
        # 此时run_in_executor只需传绑定后的函数，无需额外参数
        result = await loop.run_in_executor(self.executor, bound_func)
        return result
    
    # 通用异步协程：调用阻塞函数（通过线程池）
    async def async_wrapper(self,func,*args,**kws ):
        '''将阻塞函数提交进线程池(若线程池已满需排队等待直到有线程执行结束),以支持协程中使用,事件循环工作在某单个线程中(可以不属于self.executor)'''
        # 关键：用partial绑定open_close的关键字参数
        bound_func = partial(func,*args,**kws)  # 先把参数绑定到函数上
        loop = asyncio.get_running_loop()
        # 此时run_in_executor只需传绑定后的函数，无需额外参数
        result = await loop.run_in_executor(self.executor, bound_func)
        return result
    
    def auto_pos_thread(self,symbol,return_queue:Optional[queue.Queue]=None):
        '''
        智能调仓任务,自动下单,并随排队价相对报单价变动自动撤单、追单,直到持仓达到设定仓位,若是下单出错(如保证金不足)则退出调仓任务
        Args:
            symbol: 合约代码
            return_queue: 下单结果返回值队列
        '''
        if not self.check_instrument(symbol): return
        if symbol not in self._pos_queue: self._pos_queue[symbol] = queue.Queue()
        else: 
            e = f"{datetime.now()} -合约{symbol}的调仓任务已存在\n"
            self.logs_txt(e,self._logfile,_print=True)
            return
        quote = self.get_quote(symbol)
        position = self.get_position(symbol)
        instrument_property = self.get_symbol_info(symbol)
        PriceTick = instrument_property["PriceTick"]
        while True:
            if symbol not in self._pos_queue: return
            #{"pos_long":int,"pos_short":int,"price":"对手价"/"排队价/超价","open_min_volume":int,"kw":{}}
            pos = self._pos_queue[symbol].get()
            
            if isinstance(pos["pos_long"],int):
                while position.pos_long != pos["pos_long"] :
                    if position.pos_long < pos["pos_long"] : 
                        kaiping = "kaiduo"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.AskPrice1
                        elif pos['price'] == "排队价": price = quote.BidPrice1
                        elif pos['price'] == "超价": price = quote.BidPrice1 + 1 * PriceTick

                    else: 
                        kaiping = "pingduo"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.BidPrice1
                        elif pos['price'] == "排队价": price = quote.AskPrice1
                        elif pos['price'] == "超价": price = quote.AskPrice1 - 1 * PriceTick
                    lot = abs(position.pos_long - pos["pos_long"] )
                    if kaiping == "kaiduo" and lot < pos["open_min_volume"]:
                        e = f"{datetime.now()} -开仓数量:{lot},小于最低开仓数量:{pos['open_min_volume']}\n"
                        self.logs_txt(e,self._logfile,_print=True)
                        break
                    elif lot:
                        r = self.open_close(symbol,kaiping,lot,price,**pos["kw"])
                        if isinstance(return_queue,queue.Queue): return_queue.put(r)
                        if not r["shoushu"] and r["order_wrong"]:
                            e = f"{datetime.now()} -下单出错,返回值:{r}\n"
                            self.logs_txt(e,self._logfile,_print=True)
                            self._pos_queue.pop(symbol)
                            return
               
            if isinstance(pos["pos_short"],int):
                while position.pos_short != pos["pos_short"] :
                    if position.pos_short < pos["pos_short"] : 
                        kaiping = "kaikong"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.BidPrice1
                        elif pos['price'] == "排队价": price = quote.AskPrice1
                        elif pos['price'] == "超价": price = quote.AskPrice1 - 1 * PriceTick
                    else: 
                        kaiping = "pingkong"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.AskPrice1
                        elif pos['price'] == "排队价": price = quote.BidPrice1
                        elif pos['price'] == "超价": price = quote.BidPrice1 + 1 * PriceTick
                    lot = abs(position.pos_short - pos["pos_short"] )
                    if kaiping == "kaikong" and lot < pos["open_min_volume"]:
                        e = f"{datetime.now()} -开仓数量:{lot},小于最低开仓数量:{pos['open_min_volume']}\n"
                        self.logs_txt(e,self._logfile,_print=True)
                        break    
                    elif lot:
                        r = self.open_close(symbol,kaiping,lot,price,**pos["kw"])
                        if isinstance(return_queue,queue.Queue): return_queue.put(r)
                        if not r["shoushu"] and r["order_wrong"]:
                            e = f"{datetime.now()} -下单出错,返回值:{r}\n"
                            self.logs_txt(e,self._logfile,_print=True)
                            self._pos_queue.pop(symbol)
                            return
                
    async def auto_pos_async(self,symbol,return_queue:Optional[asyncio.Queue]=None):
        '''
        智能调仓任务,自动下单,并随排队价相对报单价变动自动撤单、追单,直到持仓达到设定仓位,若是下单出错(如保证金不足)则退出调仓任务
        Args:
            symbol: 合约代码
            return_queue: 下单结果返回值队列
        '''
        if not self.check_instrument(symbol): return
        if symbol not in self._pos_queue: self._pos_queue[symbol] = asyncio.Queue()
        else: 
            e = f"{datetime.now()} -合约{symbol}的调仓任务已存在\n"
            self.logs_txt(e,self._logfile,_print=True)
            return
        loop = asyncio.get_running_loop()
        quote = await loop.create_task(self.async_wrapper(self.get_quote,symbol))
        position = await loop.create_task(self.async_wrapper(self.get_position,symbol))
        instrument_property = self.get_symbol_info(symbol)
        PriceTick = instrument_property["PriceTick"]
        while True:
            if symbol not in self._pos_queue: return
            #{"pos_long":int,"pos_short":int,"price":"对手价"/"排队价","open_min_volume":int,"kw":{}}
            pos = await self._pos_queue[symbol].get()
            if isinstance(pos["pos_long"],int):
                while position.pos_long != pos["pos_long"] :
                    if position.pos_long < pos["pos_long"] : 
                        kaiping = "kaiduo"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.AskPrice1
                        elif pos['price'] == "排队价": price = quote.BidPrice1
                        elif pos['price'] == "超价": price = quote.BidPrice1 + 1 * PriceTick
                    else: 
                        kaiping = "pingduo"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.BidPrice1
                        elif pos['price'] == "排队价": price = quote.AskPrice1
                        elif pos['price'] == "超价": price = quote.AskPrice1 - 1 * PriceTick
                    lot = abs(position.pos_long - pos["pos_long"] )
                    if kaiping == "kaiduo" and lot < pos["open_min_volume"]:
                        e = f"{datetime.now()} -开仓数量:{lot},小于最低开仓数量:{pos['open_min_volume']}\n"
                        self.logs_txt(e,self._logfile,_print=True)
                        break
                    elif lot:
                        r = await loop.create_task(self.OpenClose(symbol,kaiping,lot,price,**pos["kw"]))
                        if isinstance(return_queue,asyncio.Queue): await return_queue.put(r)
                        if not r["shoushu"] and r["order_wrong"]:
                            e = f"{datetime.now()} -下单出错,返回值:{r}\n"
                            self.logs_txt(e,self._logfile,_print=True)
                            self._pos_queue.pop(symbol)
                            return
            if isinstance(pos["pos_short"],int):
                while position.pos_short != pos["pos_short"] :
                    if position.pos_short < pos["pos_short"] : 
                        kaiping = "kaikong"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.BidPrice1
                        elif pos['price'] == "排队价": price = quote.AskPrice1
                        elif pos['price'] == "超价": price = quote.AskPrice1 - 1 * PriceTick
                    else: 
                        kaiping = "pingkong"
                        if pos['price'] == "对手价" or pos['price'] is None: price = quote.AskPrice1
                        elif pos['price'] == "排队价": price = quote.BidPrice1
                        elif pos['price'] == "超价": price = quote.BidPrice1 + 1 * PriceTick
                    lot = abs(position.pos_short - pos["pos_short"] )
                    if kaiping == "kaikong" and lot < pos["open_min_volume"]:
                        e = f"{datetime.now()} -开仓数量:{lot},小于最低开仓数量:{pos['open_min_volume']}\n"
                        self.logs_txt(e,self._logfile,_print=True)
                        break    
                    elif lot:
                        r = await loop.create_task(self.OpenClose(symbol,kaiping,lot,price,**pos["kw"]))
                        if isinstance(return_queue,asyncio.Queue): await return_queue.put(r)
                        if not r["shoushu"] and r["order_wrong"]:
                            e = f"{datetime.now()} -下单出错,返回值:{r}\n"
                            self.logs_txt(e,self._logfile,_print=True)
                            self._pos_queue.pop(symbol)
                            return
                
    def set_pos_volume(self, symbol, pos_long:int=None, pos_short:int=None, price:Optional[str]=None, open_min_volume:int=1, **kw):
        '''
        设置目标仓位,由调仓任务auto_pos自动把持仓调整到目标数量,设置超价则以超排队价1跳下单
        用法:
            #设置多头目标仓位100手 
            set_pos_volume(rb2601, pos_long=100 )
            #设置多头目标仓位100手,以排队价下单 
            set_pos_volume(rb2601, pos_long=100, price = "排队价" )

            #设置空头目标仓位10手
            set_pos_volume(rb2601, pos_short=10)

            #同时设置多头目标仓位100手、空头目标仓位10手,auto_pos会先完成多头调仓再完成空头调仓
            set_pos_volume(rb2601, pos_long=100, pos_short=10)

            #设置多头目标仓位10手, 最低开仓数量4手(如菜粕交易所规定最低开仓4手)
            set_pos_volume(RM601, pos_long=10, open_min_volume=4)
        Args:
            symbol: 合约代码
            pos_long: 多仓数量
            pos_short: 空仓数量
            price: 下单价格,只支持"对手价"、"排队价"、"超价",None为对手价,不支持其他指定价避免传入错误参数频繁报撤单
            open_min_volume: 开仓最小数量,交易所最低开仓数量合约可设置
            **kw: 其他参数,若设置cancel=True,则取消调仓任务
        '''
        if not self.check_instrument(symbol): return
        if symbol in self._pos_queue: 
            if pos_long is None and pos_short is None: return
            if "cancel" in kw and kw["cancel"]:
                self._pos_queue.pop(symbol)
                return
            order_data = {"symbol":symbol, "pos_long":pos_long, "pos_short":pos_short, "price":price, "open_min_volume":open_min_volume, "kw":kw}
            self._pos_queue[symbol].put_nowait(order_data)
        else: 
            e = f"{datetime.now()} -合约{symbol}智能单任务已结束\n"
            self.logs_txt(e,self._logfile,_print=True)

    #日志            
    def logs_txt(self,e,logfile="",_print=True,sf=""):
        if not logfile:
            logfile = os.path.dirname(os.path.abspath(__file__))
        if _print: print(e)
        os.makedirs(logfile,exist_ok=True)
        ss = logfile+f"\\{date.today()}{sf}.txt"
        ff = open(ss,mode="a+",encoding='utf-8')
        ff.write(e)
        ff.write(f'\n{"-"*30}\n')
        ff.close()

    def trade_excel(self,new_df:polars.DataFrame,direction,filename, logfile = ""):
        '''保存成交记录为csv,查看文件时建议复制一份再查看,避免对文件改动引起后续写入出错'''
        if not logfile:
            logfile = os.path.dirname(os.path.abspath(__file__))
        #logfile = r'C:\TqLogs'  #指定目录
        os.makedirs(logfile,exist_ok=True)
        #"""
        file_path = fr'{logfile}\策略{direction}统计-{filename}.csv'
        include_header = not os.path.exists(file_path)
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                new_df.write_csv( f, include_header=include_header )
        except Exception as e: #文件被占用或其他原因无法写入
            self.logs_txt(f"保存成交记录失败,原因:{e}",file_path)
            file_path = fr'{logfile}\策略{direction}统计-{filename}{datetime.today()}.csv'
            with open(file_path, "a", encoding="utf-8") as f:
                new_df.write_csv( f, include_header=include_header )

    def logs_excel(self,new_df:polars.DataFrame,filename,logfile=""):
        '''保存每一个策略的盈亏统计'''
        if not logfile:
            logfile = os.path.dirname(os.path.abspath(__file__))
        os.makedirs(logfile,exist_ok=True)
        file_path = fr"{logfile}\策略盈亏统计-{filename}.csv"
        include_header = not os.path.exists(file_path) 
        try:
            with open(file_path, "a", encoding="utf-8") as f:
                new_df.write_csv( f, include_header=include_header)
        except Exception as e: #文件被占用或其他原因无法写入
            self.logs_txt(f"保存策略盈亏统计失败,原因:{e}",file_path)
            file_path = fr"{logfile}\策略盈亏统计-{filename}{datetime.today()}.csv"
            with open(file_path, "a", encoding="utf-8") as f:
                new_df.write_csv( f,  include_header=include_header )

    def save_json(self,filehand,datadict):
        #保存本地文件
        filehand.seek(0) #
        filehand.truncate() #
        json.dump(datadict, filehand,indent=4) 
        filehand.flush() #

    #发邮件
    def send_email(self,sender,authorization,recver_list,title,content):
        try: 
            #sender = "2522729256@qq.com"  #发送方
            #authorization = "你的自己的SMTP授权码" #SMTP授权码，不是邮箱密码
            #recver_list = ["2522729256@qq.com"] #接收方列表
            #title = "算法通知"         #邮件标题
            if isinstance(recver_list,str): recver_list = [recver_list]
            message = MIMEText(content, "plain", "utf-8")  #"plain"文本格式  
            message['Subject'] = title                   #邮件标题
            message['From'] = sender                       #发件人
            message['To'] = ",".join(recver_list)         # 收件人列表
            #windows server 2012 服务器上的程序发送smtp邮件必须通过SSL协议端口发送
            #smtp = smtplib.SMTP("smtp.qq.com",465,timeout=3)
            smtp = smtplib.SMTP_SSL("smtp.qq.com",465,timeout=3) #实例化smtp服务器 163邮箱用465或994端口邮件服务商的加密465端口，否则用25端口
            smtp.login(sender, authorization)               #发件人登录
            smtp.sendmail(sender, recver_list,
                            message.as_string())     # as_string 对 message 的消息进行了封装
            smtp.quit()                  
        except: pass
        finally:
            smtp.close()
    # 发消息
    def send_msg(self,DingDing,WeChat,QQemail,chan:zhuchannel.ThreadChan,logfile,_print=False,sf="") :
        for content in chan:
            try:
                headers = {"content-type": "application/json;charset=utf-8"}
                if  DingDing: 
                    msg = {"msgtype": "text",
                        "text": {"content": "-{}\n{}".format(DingDing[0],content)}}
                    body = json.dumps(msg)
                    requests.post(DingDing[1], data=body, headers=headers,timeout=3 )
                if  WeChat: 
                    msg = {"msgtype": "text",
                        "text": {"content": "-{}\n{}".format(WeChat[0],content)}}
                    body = json.dumps(msg)
                    requests.post(WeChat[1], data=body, headers=headers,timeout=3 )
                if QQemail: self.send_email(*QQemail,content)
                #self.logs_txt(content,s)
                tm.sleep(4)
            except Exception: 
                e = traceback.format_exc()
                self.logs_txt(e,logfile,_print,sf)

    def send_message(self,DingChan:zhuchannel.ThreadChan,account,orderrisk={},r={},name="",e="",**kw):
        if isinstance(DingChan,zhuchannel.ThreadChan): 
            if r:
                position = r['position']
                content = "{}{}{}{}{}".format(
                                    f"{datetime.now()}-{e}\n策略-{name}\n{r['symbol']} {r['kaiping']},下单手数:{r['lot']},下单价格:{r['price']},成交手数:{r['shoushu']},成交均价:{r['junjia']},",
                                    f"{r['last_msg']},\n{'空头' if 'kong' in r['kaiping'] else '多头'}持仓:{position['pos_short'] if 'kong' in r['kaiping'] else position['pos_long']}\n",
                                    f"账户权益:{round(account['Balance'],2)},账户可用资金:{round(account['Available'],2)},账户保证金:{round(account['CurrMargin'],2)},持仓浮盈:",
                                    f"{round(account['float_profit'],2)},风险度:{round(account['risk_ratio'],2)},\n",
                                    f"\n报撤单统计:{orderrisk}")
            else:
                content = "{}{}{}{}".format(
                                    f"{datetime.now()}-{e}\n策略-{name} ",
                                    f"账户权益:{round(account['Balance'],2)},账户可用资金:{round(account['Available'],2)},账户保证金:{round(account['CurrMargin'],2)},持仓浮盈:",
                                    f"{round(account['float_profit'],2)},风险度:{round(account['risk_ratio'],2)},\n",
                                    f"报撤单统计:{orderrisk}")
            ln = len(content)
            lln = 2000 if "msg" not in  kw else kw["msg"]
            num = int(ln/lln) + 1 if ln/lln - int(ln/lln) > 0 else int(ln/lln)
            for i in range(1,num+1):
                DingChan.put_nowait(f"第{i}部分,共{num}部分\n\n" + content[(i-1)*lln:lln*i])



