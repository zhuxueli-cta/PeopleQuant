#!/usr/bin/env python
#  -*- coding: utf-8 -*-
#import thosttraderapi 
import ctpapi
import time as tm
import zhuchannel
import threading,queue
import os
from concurrent.futures import ThreadPoolExecutor
import traceback
import types
from functools import partial
import polars
from datetime import datetime,time,date

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
TradeFrontAddr="tcp://180.168.146.187:10101"
MdFrontAddr="tcp://101.230.209.178:53313"
TradeFrontAddr = envs["电信1"]["td"]
MdFrontAddr = envs["电信1"]["md"]
TradeFrontAddr = envs["7x24"]["td"]
MdFrontAddr = envs["7x24"]["md"]

#TradeFrontAddr = "tcp://121.37.80.177:20002" #openctp
#MdFrontAddr = "tcp://121.37.80.177:20004" #openctp

BROKERID="9999"
USERID=""
#USERID=""
PASSWORD=""
APPID="simnow_client_test"
AUTHCODE="0000000000000000"


#日志            
def logs_txt(e,logfile,_print=True):
    if _print: print(e)
    os.makedirs(logfile,exist_ok=True)
    ss = logfile+f"\\{date.today()}.txt"
    ff = open(ss,mode="a+",encoding='utf-8')
    ff.write(e)
    ff.write(f'\n{"-"*30}\n')
    ff.close()

class PeopleQuantApi():
    def __init__(self,BrokerID=BROKERID,UserID=USERID,PassWord=PASSWORD,AppID=APPID, AuthCode=AUTHCODE,TradeFrontAddr=TradeFrontAddr,MdFrontAddr=MdFrontAddr,
                 s=USERID,quote_queue:zhuchannel.ThreadChan=None):
        ctpapi.check_password(1)
        self._subscribequeue = zhuchannel.ThreadChan() #mdapi从各层包括tradeapi接收订阅、退订
        self._quote_queue = zhuchannel.ThreadChan() #mdapi向tradeapi发送行情
        self.quote_queue = quote_queue #各层从mdapi接收行情
        self._Reqqueue = zhuchannel.ThreadChan() #请求队列
        self.MarketDataqueue = []
        self.MarketDataqueue.append(self._quote_queue)
        if quote_queue is not None:
            self.MarketDataqueue.append(quote_queue)
        self.instruments = {}  #全部合约代码
        self.quote = {}  #全部合约行情
        self.instruments_property = {} #合约属性
        self.positions = {}  #全部合约持仓
        self.orders = {}  #全部委托单
        self.trades = {}  #全部成交单
        self.account= {}  #账户资金
        self._exception_queue = zhuchannel.ThreadChan()  # 用于传递异常的队列
        self._rtn_queue = zhuchannel.ThreadChan()  # 用于接收CTP主动发出的回报队列
        self._ans_queue = zhuchannel.ThreadChan( )  # 用于接收CTP反馈的结果队列
        self._ReqID = 0  #请求编号

        self._tradeargs = tuple()
        self._tradekwargs = {"BrokerID":BrokerID,"UserID":UserID,"Password":PassWord,"AppID":AppID, "AuthCode":AuthCode,"s":s,"Subscribequeue":self._subscribequeue,
                             "quote_queue":self._quote_queue,"Reqqueue":self._Reqqueue,"Notify":{"rtn_queue":self._rtn_queue,"ans_queue":self._ans_queue},
                             "TradeFrontAddr":TradeFrontAddr,"PQexception_queue":self._exception_queue,}
        self._mdargs = tuple()
        self._mdkwargs = {"BrokerID":BrokerID,"UserID":UserID,"Password":PassWord,"AppID":AppID, "AuthCode":AuthCode,"s":s,"Subscribequeue":self._subscribequeue,
                          "MarketDataqueue":self.MarketDataqueue,"Notify":{"rtn_queue":self._rtn_queue,"ans_queue":self._ans_queue},
                          "MdFrontAddr":MdFrontAddr}
        
        self.tradethread = zhuchannel.WorkThread(target=ctpapi.TraderApi,args=tuple(),kwargs=self._tradekwargs,_api=True)
        self.mdthread = zhuchannel.WorkThread(target=ctpapi.MdApi,args=tuple(),kwargs=self._mdkwargs,_api=True)
        self.tradethread.start()
        self.mdthread.start()
        self.rtnthread = zhuchannel.WorkThread(target=self._rtn_thread,args=tuple(),kwargs={},exception_queue=self._exception_queue )
        self.rtnthread.start()
        self.exception = zhuchannel.WorkThread(target=self.exception_thread,args=tuple(),kwargs={} )
        self.exception.start()
        self.executor = ThreadPoolExecutor()
        try:
            r1 = self._ans_queue.get(timeout=60) #等待交易初始化完成
            r2 = self._ans_queue.get(timeout=60) #等待行情初始化完成
            print("交易接口,行情接口:初始化完成")
        except Exception :
            e = "初始化失败,可能网络连接超时,请检查网络"
            self._exception_queue.put(e)
            #raise Exception(e)
    def Join(self):
        self.tradethread._Join()
        self.mdthread._Join()
    def exception_thread(self):
        for e in self._exception_queue:
            e = f'\n{datetime.today()}{"-"*30}+\n{e}\n'
            logs_txt(e,self.tradethread.api._logfile)
            if self.rtnthread.is_alive(): self._rtn_queue.put("exception")
            self.Join()
            self.tradethread.join()
            self.mdthread.join()
            print(self.tradethread.is_alive(),self.mdthread.is_alive())
            threads = threading.enumerate()
            for idx, thread in enumerate(threads):
                print(f"线程 {idx + 1}:")
                print(f"  名称: {thread.name}")
                print(f"  ID: {thread.ident}")  # 线程ID（可能为None，未启动时）
                print(f"  是否存活: {thread.is_alive()}")
                print(f"  是否为守护线程: {thread.daemon}")
            raise Exception(e)
            for t in self.threads:
                    t.join(timeout=1.0)
                    if t.is_alive():
                        print(f"线程 {t.name} 无法优雅终止，已强制中断")
                    else:
                        print(t.is_alive(),t.name,"死了")
    
    def get_account(self,wait_return=False):
        if self.account: return self.account
        else:
            r = self._sendReq({"reqfuncname":"get_account","wait_return":wait_return, })
            if not r['ret'] : print("查询账户失败")
            else: 
                self.account = r['ret'] 
                return r['ret'] 
            
    def get_position(self,InstrumentID:str="",wait_return=False,_ctp = None):
        if InstrumentID and not self.check_instrument(InstrumentID): return
        if InstrumentID in self.positions:
            return self.positions[InstrumentID]
        else:
            r = self._sendReq({"reqfuncname":"get_position","InstrumentID":InstrumentID,"wait_return":wait_return,"_ctp":_ctp, })
            if not r['ret']: print("查询持仓失败")
            elif InstrumentID:
                self.positions[InstrumentID] = r['ret']
                return self.positions[InstrumentID]
            else: 
                self.positions.update(r['ret'])
                return self.positions
    
    def get_quote(self,InstrumentID:str,wait_return=True):
        if not self.check_instrument(InstrumentID): return
        if InstrumentID not in self.instruments_property:
            self.get_symbol_info(InstrumentID)
        if InstrumentID in self.quote:
            return self.quote[InstrumentID]
        elif InstrumentID:
            #print(("发送了行情查询get_quote",InstrumentID))
            q = self.subscribe_quote([InstrumentID]) #订阅行情
            r = self._sendReq({"reqfuncname":"get_quote","InstrumentID":InstrumentID,"wait_return":wait_return, })
            if not r['ret'] : 
                print("查询行情失败")
            else:
                while InstrumentID not in self.quote:  #等待行情更新
                    tm.sleep(0.1)
                    #print(("等待行情返回",list(self.quote)))
                else: return self.quote[InstrumentID]

    def get_symbol_trade(self,InstrumentID:str = "",wait_return=False) -> list:
        '''
        获取合约成交单
        Ags:
            InstrumentID: 合约代码,如rb2601,不填则查询全部成交单
        Return:
            由成交单字典组成的列表
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_trade","InstrumentID":InstrumentID,"wait_return":wait_return})
        if not r['ret']: print("get_symbol_trade查询成交单为空")
        return r['ret']

    def get_symbol_order(self,InstrumentID:str = "", OrderStatus:str = "",CombOffsetFlag:str="",wait_return=False) ->list:
        '''
        获取合约委托单
        Ags:
            InstrumentID: 合约代码,如rb2601,不填则查询全部委托单
            OrderStatus: 委托单状态,"Alive"查询活动委托单,"Finished"查询已完结委托单
            CombOffsetFlag: Open开仓单,Close平仓单
        Return:
            由委托单字典组成的列表
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_order","InstrumentID":InstrumentID,"OrderStatus":OrderStatus,"CombOffsetFlag":CombOffsetFlag,"wait_return":wait_return })
        if not r['ret']: print("get_symbol_order查询委托单为空")
        return r['ret']
            
    def get_id_order(self,InstrumentID:str="", order_id:str="",wait_return=False) ->list:
        '''
        获取合约委托单
        Ags:
            InstrumentID: 合约代码,如rb2601
            order_id: 为委托单order编号组成的字符串:f'{order["FrontID"]}_{order["SessionID"]}_{order["OrderRef"]}'
        Return:
            委托单字典,或委托单字典组成的列表
        '''
        #raise Exception("哪里有调用")
        if InstrumentID and not self.check_instrument(InstrumentID): return
        if order_id in self.orders:
            return self.orders[order_id]
        else:
            r = self._sendReq({"reqfuncname":"get_id_order","InstrumentID":InstrumentID,"order_id":order_id,"wait_return":wait_return, })
            if not r['ret']: print("get_id_order查询委托单为空")
            return r['ret']
            
    def get_trade_of_order(self, order_id:str,wait_return=False):
        '''
        获取合约委托单对应的成交单
        Ags:
            InstrumentID: 必填,合约代码
            OrderRef: 必填,委托单OrderRef
        Return:
            由成交单字典组成的列表
        '''
        r = self._sendReq({"reqfuncname":"get_trade_of_order","order_id":order_id,"wait_return":wait_return, })
        if not r['ret']: print("get_trade_of_order查询成交单失败")
        return r['ret']
    
    def get_order_risk(self,InstrumentID:str = "",wait_return=False) ->dict:
        '''
        获取合约委托单风控统计
        Ags:
            InstrumentID: 合约代码,如rb2601
        Return:
            字典,健为:报单次数、撤单次数、开仓成交手数
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_order_risk","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: print("查询委托单风控失败")
        return r['ret']

    def get_frozen_pos(self,InstrumentID:str,wait_return=False) -> dict:
        '''
        获取合约冻结仓位,上期所、能源中心区分昨仓、今仓,其他交易所只有昨仓冻结
        由活动委托单计算得出
        Return:
            {"short_frozen_today":0,"long_frozen_today":0,
            "short_frozen_his":0,"long_frozen_his":0}
        '''
        if not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_frozen_pos","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: print("查询冻结持仓失败")
        return r['ret']
    
    def get_frozen_margin(self,InstrumentID:str="",wait_return=False) -> dict:
        '''
        获取冻结保证金,按多空活动开仓单保证金求和,默认多空开仓单都占用保证金,不考虑保证金优惠
        Args:
            InstrumentID: 合约代码,如rb2601,不填则查询账户冻结保证金
        Return:
            保证金之和
        '''
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_frozen_margin","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: print("查询冻结保证金失败")
        return r['ret']
        
    def get_symbol_info(self,InstrumentID:str,wait_return=False):
        '''
        获取单个具体合约(期货或期权)属性
        Args:
            InstrumentID:str,合约代码,若查询合约不存在返回空表格
        Return:
            polars.DataFrame表格
            若查询不存在返回空表格
        '''
        if not self.check_instrument(InstrumentID): return
        if InstrumentID in self.instruments_property:
            return self.instruments_property[InstrumentID]
        else:
            r = self._sendReq({"reqfuncname":"get_symbol_info","InstrumentID":InstrumentID,"wait_return":wait_return, })
            self.instruments_property[InstrumentID] = r['ret']
            return r['ret']
    def query_symbol(self,ExchangeID:str,ProductID:str,ProductClass:str,OptionsType:str) -> polars.DataFrame:
        '''
        根据交易所ID或品种ID查询合约属性,可查询期货、期权合约属性
        Args:
            ExchangeID:str,交易所代码,上期所SHFE,大商所DCE,郑商所CZCE,能源中心INE,中金所CFFEX,广期所GFEX
                    不填则查找全部交易所
            ProductID:str,品种ID,如rb
            ProductClass:str,查找合约类型,期货"Future",期权"Option"
            OptionsType:str,期权类型,看涨期权CALL,看跌期权PUT
        若查询不存在返回空表格
        '''
        r = self._sendReq({"reqfuncname":"query_symbol","ExchangeID":ExchangeID,"ProductID":ProductID,"ProductClass":ProductClass,
                            "OptionsType":OptionsType,})
        return r['ret']
    def query_symbol_option(self,UnderlyingInstrID:str,OptionsType:str) -> polars.DataFrame:
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
            └───────────┴─────────────────┴────────────┴───────────────┴───┴─────────────┴───────────────────┴────────────────────┴────────────────┘)
        '''
        r = self._sendReq({"reqfuncname":"query_symbol_option","UnderlyingInstrID":UnderlyingInstrID,"OptionsType":OptionsType,})
        return r['ret']
    def get_option(self,option_class,underlying_price,price_level,group_option:polars.DataFrame) -> dict:
        '''
        查询以价格underlying_price为基准的档位price_level对应的期权
        group_option: 由query_symbol_option返回的期权表格
        ('SR511P5500', 5500.0, 100.0)
        '''
        r = self._sendReq({"reqfuncname":"get_option","option_class":option_class,"underlying_price":underlying_price,
                            "price_level":price_level,"group_option":group_option,})
        return r['ret']

    def get_symbol_marginrate(self,InstrumentID:str,wait_return=False):
        if InstrumentID and not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"get_symbol_marginrate","InstrumentID":InstrumentID,"wait_return":wait_return, })
        if not r['ret']: print("查询保证金失败")
        return r['ret']
        
    def _wait_order(self,ReqID:int):
        try:
            r = self._ans_queue.get_nowait()
            if ReqID in r: return r[ReqID] #查询完成
            else: self._ans_queue.put_nowait(r)
        except queue.Empty:
            return True
 
    def cancel_order(self, order_id:str,wait_return=False):
        '''阻塞函数,等待撤单结束'''
        if order_id in self.orders and self.orders[order_id]["OrderStatus"] in ['0','4' ,'5' ]: return
        r = self._sendReq({"reqfuncname":"ReqOrderAction","order_id":order_id,"wait_return":wait_return,})
        if not r['ret'] :
            print("撤单失败")
            return
        else: #撤单发出但不代表被交易所接受
            return
            

    def insert_order(self,ExchangeID:str,InstrumentID:str,Direction:str,Offset:str,Volume:int,LimitPrice:float,advanced=None,HedgeFlag:str="1",WaitReturn=False):
        '''非阻塞函数,报单被交易所接受后返回字典'''
        if not self.check_instrument(InstrumentID): return
        r = self._sendReq({"reqfuncname":"insert_order","ExchangeID":ExchangeID,"InstrumentID":InstrumentID,"Direction":Direction,"Offset":Offset,"Volume":Volume,"LimitPrice":LimitPrice,
                            "advanced":advanced,"HedgeFlag":HedgeFlag,"WaitReturn":WaitReturn,})
        if not r['ret'] :
            print("报单失败,客户端拒绝")
            return
        else:  #{order_id:order}
            for order_id in r['ret']:
                if order_id not in self.orders: self.orders[order_id] = r['ret'][order_id]
            #print((r['ret'],"insert_order"))
            #报单发出成功
            return self.orders[order_id]

    def open_close(self,symbol,kaiping='',lot=0,price=None,block=True,n_price_tick=1,che_time=0,order_info='无',signal_price=float('nan'),order_close_chan=True,HedgeFlag:str="1",WaitReturn=False,**kw):
        order_wrong = False #是否错单
        quote = self.get_quote(symbol)
        #symbol = quote["InstrumentID"] #合约代码  
        ExchangeID = quote["ExchangeID"] #交易所代码
        position = self.get_position(symbol)
        instrument_property = self.get_symbol_info(symbol)
        PriceTick = instrument_property["PriceTick"]
        VolumeMultiple = instrument_property["VolumeMultiple"]
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
        
        lot = min(lot, instrument_property["MaxLimitOrderVolume"])
        lot = int(lot)
        if (price_buy/PriceTick - int(price_buy/PriceTick) > 0 or price_sell/PriceTick - int(price_sell/PriceTick) > 0 or
            lot <= 0 or lot < instrument_property["MinLimitOrderVolume"]): 
            #以持仓数量平仓时平仓数量为0可能是服务器故障持仓未更新(也可能其他程序超额平仓,或清仓代码正常所需非本策略bug),等待更新后可继续下单,其他情况下的报价和手数错误应退出交易
            #if not (lot == position["pos_long"] == 0 and kaiping == 'pingduo' or lot == position["pos_short"] == 0 and kaiping=='pingkong'): 
            order_wrong = True 
            return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                    "last_msg":f"报单错误,下单价格{price},下单手数{lot},最大限价单{instrument_property['MaxLimitOrderVolume']},最小限价单{instrument_property['MinLimitOrderVolume']}" ,
                    "quote_volume":quote_volume,"profit_count":0,"profit_money":0, "order_id":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
    
        shoushu = 0 #已成交手数  
        junjia = 0.0 #成交均价 
        che_count, day_order = 0, 0 #报撤单次数
        profit_count, profit_money = 0, 0 #平仓盈亏点数和盈亏金额
        ping_jin,ping_zuo,order = None,None,None  #委托单对象
        last_msg, order_id = "", []  #委托单状态信息和单号
        if kaiping== 'pingduo': #交易方向为平多
            #可能服务器故障持仓未更新,等待更新
            if not position["pos_long"] or position["open_price_long"] != position["open_price_long"]:
                return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                        "last_msg":f"多头持仓错误,多头持仓手数{position['pos_long']},多头持仓价格{position['open_price_long']}" ,
                        "quote_volume":quote_volume,"profit_count":0,"profit_money":0, "order_id":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
            pos_direction = "多" #原持仓方向
            pos0 = position["pos_long"] #原多头持仓手数
            price0 = position["open_price_long"] #原多头开仓均价
            #margin0 = position.margin_long #原多头占用保证金
            if 0 < lot <= position["pos_long_today"] : #小于等于今仓，平今
                ping_jin=self.insert_order(ExchangeID,symbol,'Sell','CloseToday', lot, price_sell)
            elif 0 < position["pos_long_today"] < lot <= position["pos_long"]:  #优先平今,剩余仓位平昨   
                ping_zuo=self.insert_order(ExchangeID,symbol,'Sell','Close',lot-position["pos_long_today"],price_sell) #先平昨再平今 
                ping_jin=self.insert_order(ExchangeID,symbol,'Sell','CloseToday',position["pos_long_today"],price_sell)
            elif 0 == position["pos_long_today"] < lot <= position["pos_long"]: #只有昨仓
                ping_zuo=self.insert_order(ExchangeID,symbol,'Sell','Close',lot,price_sell) 
        elif kaiping=='pingkong': #交易方向为平空
            if not position["pos_short"] or position["open_price_short"] != position["open_price_short"]:
                return {"shoushu":0, "junjia":float("nan"), "che_count":0, "day_order":0, "symbol":symbol, "kaiping":kaiping,"lot":lot,"price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell,
                        "last_msg":f'空头持仓错误,空头持仓手数{position["pos_short"]},空头持仓价格{position["open_price_short"]}' ,
                        "quote_volume":quote_volume,"profit_count":0,"profit_money":0, "order_id":[],"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position}
            pos_direction = "空" #原持仓方向
            pos0 = position["pos_short"] #原空头持仓手数
            price0 = position["open_price_short"] #原空头开仓均价
            #margin0 = position.margin_short #原空头占用保证金
            if 0 < lot <= position["pos_short_today"] : #小于等于今仓，平今
                ping_jin=self.insert_order(ExchangeID,symbol,'Buy','CloseToday',lot,price_buy)    
            elif 0 < position["pos_short_today"] < lot <= position["pos_short"]:      
                ping_zuo=self.insert_order(ExchangeID,symbol,'Buy','Close',lot-position["pos_short_today"],price_buy)      
                ping_jin=self.insert_order(ExchangeID,symbol,'Buy','CloseToday',position["pos_short_today"],price_buy)
            elif 0 == position["pos_short_today"] < lot <= position["pos_short"]:      
                ping_zuo=self.insert_order(ExchangeID,symbol,'Buy','Close',lot,price_buy)
                
        elif kaiping== 'kaiduo': #交易方向为开多
            order = self.insert_order(ExchangeID,symbol,'Buy','Open',lot,price_buy)   
        elif kaiping=='kaikong': #交易方向为开空
            order = self.insert_order(ExchangeID,symbol,"Sell","Open",lot,price_sell)
        t = datetime.now().timestamp() #时间起点
        finished = ['0','4' ,'5' ]
        if ping_zuo is not None: #报单成功发向交易所
            orderid = ping_zuo["order_id"]
            order_id.append(orderid)
            while True:
                ping_zuo = self.get_id_order(order_id=orderid)
                trades_list = self.get_trade_of_order(orderid)
                if ping_zuo["OrderStatus"] not in finished or ping_zuo["VolumeTraded"] != sum( [trade["Volume"] for trade in trades_list]): #平昨单是否完成
                    if not advanced and not block :break #当日有效单，且无需等待是否完成
                    quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                    #quote_time = (time_to_datetime(quote.datetime)+timedelta(minutes=close_minutes)).time()
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if ping_zuo["OrderStatus"] not in finished and not advanced and (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= ping_zuo["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= ping_zuo["LimitPrice"]-PriceTick*n_price_tick)) 
                    #and last_price != quote.last_price == quote.last_price 
                        #    or (close_minutes and trading_time['night'] and ( time_list[0] >= 24 and time(8) > quote_time >= night_end_time or time_list[0] < 24 and quote_time >= night_end_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                else: break
            day_order += 1 #报单数加1
            volume = ping_zuo["VolumeTraded"] #成交手数
            if ping_zuo["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += ping_zuo["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]

        if ping_jin is not None: #报单成功发向交易所
            orderid = ping_jin["order_id"]
            order_id.append(orderid)
            while True:
                ping_jin = self.get_id_order(order_id=orderid)
                trades_list = self.get_trade_of_order(orderid)
                if ping_jin["OrderStatus"] not in finished or ping_jin["VolumeTraded"] != sum( [trade["Volume"] for trade in trades_list]): #平昨单是否完成
                    if not advanced and not block :break #当日有效单，且无需等待是否完成
                    quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                    #quote_time = (time_to_datetime(quote.datetime)+timedelta(minutes=close_minutes)).time()
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if ping_jin["OrderStatus"] not in finished and not advanced and (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= ping_jin["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= ping_jin["LimitPrice"]-PriceTick*n_price_tick)) 
                    #and last_price != quote.last_price == quote.last_price 
                        #    or (close_minutes and trading_time['night'] and ( time_list[0] >= 24 and time(8) > quote_time >= night_end_time or time_list[0] < 24 and quote_time >= night_end_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                else: break
            day_order += 1 #报单数加1
            volume = ping_jin["VolumeTraded"] #成交手数
            if ping_jin["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += ping_jin["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]

        if order is not None: #报单成功发向交易所
            orderid = order["order_id"]
            order_id.append(orderid)
            while True:
                order = self.get_id_order(order_id=orderid)
                trades_list = self.get_trade_of_order(orderid)
                if order["OrderStatus"] not in finished or order["VolumeTraded"] != sum( [trade["Volume"] for trade in trades_list]): #平昨单是否完成
                    if  not block :break #当日有效单，且无需等待是否完成
                    quote_volume = quote["AskVolume1"] if kaiping in ["kaiduo","pingkong"] else quote["BidVolume1"]
                    #quote_time = (time_to_datetime(quote.datetime)+timedelta(minutes=close_minutes)).time()
                    #等待che_time秒还不成交撤单，或者价格偏离委托价n_price_tick不成交撤单，适用于advanced=None的情况
                    if order["OrderStatus"] not in finished and  (int(che_time)>0 and datetime.now().timestamp() - t >= che_time or int(n_price_tick)>0 and 
                    ((kaiping in ["kaiduo","pingkong"] and quote["BidPrice1"] >= order["LimitPrice"]+PriceTick*n_price_tick) or
                    (kaiping in ["kaikong","pingduo"] and quote["AskPrice1"] <= order["LimitPrice"]-PriceTick*n_price_tick)) 
                    #and last_price != quote.last_price == quote.last_price 
                        #    or (close_minutes and trading_time['night'] and ( time_list[0] >= 24 and time(8) > quote_time >= night_end_time or time_list[0] < 24 and quote_time >= night_end_time))
                    ):
                        self.cancel_order(orderid)  #等待撤单完成，防止重复撤单
                else: break
            day_order += 1 #报单数加1
            volume = order["VolumeTraded"] #成交手数
            if order["VolumeTotal"] > 0: che_count += 1 #撤单次数增加
            last_msg += order["StatusMsg"]
            if volume > 0: #有成交
                shoushu += volume #计算已成交手数
                for trade in trades_list:
                    junjia += trade["Volume"]*trade["Price"]
        if shoushu: 
            junjia = junjia/shoushu #计算成交均价
            #junjia = round(junjia/shoushu,quote.price_decs) #保留和报价同样小数位
            if kaiping in ['pingduo','pingkong'] and order_close_chan:
                if kaiping== 'pingduo':
                    pos_c = position["pos_long"] #平仓后剩余多头手数
                    price_c = position["open_price_long"] #平仓后剩余多头开仓均价
                    #margin_c = position.margin_long #平仓后剩余多头保证金
                elif kaiping=='pingkong':
                    pos_c = position["pos_short"] #平仓后剩余空头手数
                    price_c =position["open_price_short"] #平仓后剩余空头开仓均价
                    #margin_c = position.margin_short #平仓后剩余空头保证金
                if pos_c > 0:
                    price_o = (pos0*price0 - pos_c*price_c)/shoushu #被平仓的理论开仓均价
                else :price_o = price0
                if kaiping=='pingduo':
                    profit_count = junjia - price_o #盈利点数
                elif kaiping=='pingkong': 
                    profit_count = price_o - junjia #盈利点数
                count_percent = round(profit_count/price_o,5) if price_o else 0 #盈利点率
                #margin_release = (margin0 if margin0==margin0 else 0) - (margin_c if margin_c==margin_c else 0)#释放保证金,平仓释放的保证金为理论投入资金
                #margin_release = (margin0 if margin0==margin0 else 0) * shoushu/pos0  #释放保证金，按仓位比计算，避免外部平仓产生影响
                profit_money = shoushu * profit_count * VolumeMultiple #盈利金额
                
        else: #错单原因
            junjia = float('nan')
            #集合竞价,未到开盘时间等待60秒
            if "拒绝" in last_msg and ("竞价" in last_msg or "竟价" in last_msg or "交易时间" in last_msg): tm.sleep(60)
            elif ("拒绝" in last_msg or "限制" in last_msg or "不足" in last_msg or "超过" in last_msg or "不支持" in last_msg or "低于" in last_msg
                  or "权限" in last_msg or "平仓" in last_msg or "开仓" in last_msg):
                order_wrong = True
        return {"shoushu":shoushu, "junjia":junjia, "che_count":che_count, "day_order":day_order, "symbol":symbol, "kaiping":kaiping, "lot":lot, "price":price_buy if kaiping in ["kaiduo","pingkong"] else price_sell, "last_msg":last_msg, 
                "quote_volume":quote_volume,"profit_count":profit_count, "profit_money":profit_money, "order_id":order_id,"order_wrong":order_wrong,"signal_price":signal_price,"order_info":order_info,"quote":quote,"position":position} #返回成交手数、成交均价,和主动撤单次数，若无成交则均价为nan值
        
    def _rtn_thread(self):
        for i in self._rtn_queue:
            if i == "exception":  
                return
            if isinstance(i,dict):
                if "instruments" in i:
                    self.instruments = i["instruments"]
                elif "pos" in i:
                    for s in i["pos"]:
                        if s not in self.positions: self.positions[s] = i["pos"][s]
                        else: self.positions[s].update(i["pos"][s])
                elif "quote" in i:
                    if i["quote"]["InstrumentID"] not in self.quote: self.quote[i["quote"]["InstrumentID"]] = i["quote"]
                    else: self.quote[i["quote"]["InstrumentID"]].update(i["quote"])
                elif "account" in i:
                    self.account.update(i["account"])
                elif "order" in i:
                    for order_id in i["order"]:
                        if order_id not in self.orders: self.orders[order_id] = i["order"][order_id]
                        else: self.orders[order_id].update(i["order"][order_id]) 
                elif "trade" in i:
                    for trade_id in i["trade"]:
                        if trade_id not in self.trades: self.trades[trade_id] = i["trade"][trade_id]
                        else: self.trades[trade_id].update(i["trade"][trade_id]) 
                elif "orderaction" in i:
                    for order_id in i["orderaction"]:
                        if order_id not in self.orders: self.orders[order_id] = i["order"][order_id]
                        else: self.orders[order_id].update(i["order"][order_id]) 
    def _ans_thread(self):
        for i in self._ans_queue:
            if "instruments" in i:
                self.instruments = i["instruments"]
            elif "pos" in i:
                self.positions.update()
    def check_instrument(self,InstrumentID:str):
        if InstrumentID in self.instruments: return True
        else: 
            print(f"合约{InstrumentID}不存在")
            raise Exception(f"合约{InstrumentID}不存在")
            return False
    
    
    def _sendReq(self,kw):
        '''
        不需要向ctp查询的直接收到的是结果,需要向ctp查询的首先收到的是发送成败
        r[ReqID]True和False表示向CTP发送成功或失败,r["return"]表示r[ReqID]为最终结果
        '''
        self._ReqID += 1
        ReqID = self._ReqID
        self._Reqqueue.put({**kw,"ReqID":ReqID})
        while True:
            r = self._ans_queue.get()
            #print((r,4564564564566456456,kw,ReqID))
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
                print("合约码为空,订阅行情失败")
                return
        #for s in InstrumentID:
        #    self.reqry_market_data(s)
        '''等待行情返回'''
        ss = [s for s in InstrumentID if s not in self.quote]
        self._subscribequeue.put_nowait({"Subscribe":ss})
        
    def unsubscribe_quote(self,InstrumentID:str,wait_return=False):
        if isinstance(InstrumentID,str): instrument_ids = [InstrumentID]
        elif isinstance(InstrumentID,list): instrument_ids = InstrumentID
        for s in instrument_ids:
            if not self.check_instrument(s): return
            if not s: 
                print("合约码为空,退订行情失败")
                return
        ss = [s for s in InstrumentID if s in self.quote]
        self._subscribequeue.put_nowait({"UnSubscribe":ss})
        for s in ss: self.quote.pop(s)

    def reqry_market_data(self,InstrumentID:str,wait_return=False):
        if not self.check_instrument(InstrumentID): return
        self._ReqID += 1
        ReqID = self._ReqID
        self._Reqqueue.put({"reqfuncname":"ReqQryDepthMarketData","InstrumentID":InstrumentID,"wait_return":wait_return,"ReqID":ReqID})
        while True:
            r = self._ans_queue.get()
            if ReqID in r: break #查询完成
            else: self._ans_queue.put_nowait(r)
        if r[ReqID] is False:
            print("查询合约深度行情失败")
            return
        
    async def OpenClose(self,loop,symbol:str, kaiping: str = '', lot: int = 0, price: float = None,block: bool = True, 
                        n_price_tick: int = 1, che_time: int = 0, order_info: str = '无', signal_price: float = float('nan'),
                        order_close_chan: bool = True, HedgeFlag: str = "1", WaitReturn: bool = False,**kw):
        
        # 关键：用partial绑定open_close的关键字参数
        bound_func = partial(self.open_close, symbol=symbol, kaiping = kaiping, lot = lot, price = price, block = block,
                            n_price_tick = n_price_tick, che_time = che_time, order_info = order_info, signal_price = signal_price,
                            order_close_chan = order_close_chan, HedgeFlag = HedgeFlag, WaitReturn = WaitReturn,**kw)  # 先把参数绑定到函数上
        
        # 此时run_in_executor只需传绑定后的函数，无需额外参数
        result = await loop.run_in_executor(self.executor, bound_func)
        return result
    
    # 通用异步协程：调用阻塞函数（通过线程池）
    async def async_wrapper(self,loop,func,*args,**kws ):
        # 关键：用partial绑定open_close的关键字参数
        bound_func = partial(func,*args,**kws)  # 先把参数绑定到函数上
        
        # 此时run_in_executor只需传绑定后的函数，无需额外参数
        result = await loop.run_in_executor(self.executor, bound_func)
        return result



if __name__ == '__main__':
    p = PeopleQuantApi()
    account = p.get_account()
    position1 = p.get_position("FG601")
    quote1 = p.get_quote("FG601")
    position2 = p.get_position("rb2601")
    quote2 = p.get_quote("rb2601")
    #margin1 = p.get_symbol_marginrate("IH2509")
    margin2 = p.get_symbol_marginrate("TF2512")
    UpdateTime = 0
    
    while True:
        #break
        if UpdateTime != quote1["UpdateTime"]:
            print(quote1["InstrumentID"],quote1["LastPrice"],quote1["UpdateTime"],)
            UpdateTime = quote1["UpdateTime"]
            print((position1,position1 is p.positions["FG601"]))
            print(quote2["InstrumentID"],quote2["LastPrice"],quote2["UpdateTime"],)
            print((position2,position2 is p.positions["rb2601"]))
            print(account)
            #print(margin1)
            #print(margin2)
            #print(p.positions)
    
    