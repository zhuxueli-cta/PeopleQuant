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
import multiprocessing,traceback,json,asyncio,threading
import smtplib,requests                                 # smtp服务器
import pandas#,openpyxl # pandas数据分析库,openpyxl读写xlsx文件 #pip install pandas openpyxl  # 同时安装 pandas 和 openpyxl
from email.mime.text import MIMEText#发送文本
from email.mime.multipart import MIMEMultipart#生成多个部分的邮件体
from email.mime.application import MIMEApplication#发送图片
from datetime import datetime, time, date, timedelta, timezone
from time import sleep
from peoplequant.pqctp import PeopleQuantApi
from peoplequant import zhuchannel
import time as tm
from tqsdk import TqApi, TqAuth,TqAccount,TqChan,TqSim,TqKq,TqBacktest,BacktestFinished,TqTimeoutError
from tqsdk.tafunc import time_to_datetime
from tqsdk.ta import MA,MACD,EMA
import signal  # <--- 新增：用于捕获 Ctrl+C 信号

#from tqsdk.calendar import CHINESE_REST_DAYS #导入天勤假期列表
#REST_DAYS = [datetime.strptime(i,'%Y-%m-%d').date() for i in CHINESE_REST_DAYS] #转换成date()
from tqsdk import calendar #3.03版本使用
calendar._init_chinese_rest_days() #更新假期,3.03以上版本使用
REST_DAYS = list(calendar.rest_days_df.date.map(lambda x:x.date())) #3.03以上版本使用
#或手工添加假期
[date(2025, 1, 1), date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30), date(2025, 1, 31), date(2025, 2, 3), 
 date(2025, 2, 4), date(2025, 4, 4), date(2025, 5, 1), date(2025, 5, 2), date(2025, 5, 5), date(2025, 6, 2), 
 date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3), date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8), 
 date(2026, 1, 1), date(2026, 1, 2), date(2026, 2, 16), date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19), 
 date(2026, 2, 20), date(2026, 2, 23), date(2026, 4, 6), date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 5), 
 date(2026, 6, 19), date(2026, 9, 25), date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5), date(2026, 10, 6), 
 date(2026, 10, 7)]

#长假判断,即周六日+假日超过3天,根据当前日期datetime.today计算    
def Rest_days(days = 3, today: datetime = None):
    '''
     days (int): today是days天长假前最后一个交易日,返回True
     today (datetime.datetime): 默认当日datetime.today()
    '''
    if today == None:today = datetime.today()
    #今日是否是days天长假前最后一个交易日
    is_rest_days = all([(today+timedelta(i)).weekday() > 4 or 
                    (today+timedelta(i)).date() in REST_DAYS
                        for i in range(1,days+1)]) #自明日连续days天是周末或者是假日
    return is_rest_days

#长假判断,根据K线时间计算,主要用于回测场景
def rest_days(api,kline,days=3):
    '''今日是days天长假前最后一个交易日,返回True'''
    if api.is_changing(kline.iloc[-1],'datetime'): #对于日周期，只在交易日切换时执行
        kline_datetime = datetime.fromtimestamp(kline.iloc[-1].datetime/1e9) #K线日期转换
        is_rest_days = all([(kline_datetime+timedelta(i)).weekday() > 4 or 
                     (kline_datetime+timedelta(i)).date() in REST_DAYS
                      for i in range(1,days+1)]) #自明日连续days天是周末或者是假日
        #即周六日+假日超过days天
        return is_rest_days #今日是长假前最后一个交易日


#交易时段
day_start = time(8, 45) #上午
day_end = time(15, 20) #下午
night_start = time(20, 45) #夜盘
night_end = time(2,35) #凌晨

#交易时段函数
def Trading_time(today: datetime = None, api=None, kw={}):
    '''
     today (datetime.datetime): today是否处于交易时段,默认当日datetime.today()
    '''
    global REST_DAYS, day_start, day_end, night_start, night_end
    day_start = kw["day_start"] if "day_start" in kw else day_start
    day_end = kw["day_end"] if "day_end" in kw else day_end
    night_start = kw["night_start"] if "night_start" in kw else night_start
    night_end = kw["night_end"] if "night_end" in kw else night_end
    if today == None :today = datetime.today()
    if "manual" in kw and kw["manual"]: 
        if ((day_start <= today.time() <= day_end  or night_start <= today.time()) and today.weekday() <= 4 
            or (today.time() <= night_end and 0 < today.weekday() <= 5)):
            return True
        else: return False
    elif api is not None: #周六周日不交易但非假期,此函数还无法区分周末与假期,可能把周五晚盘和周六凌晨交易忽略
        trading_calendar = api.get_trading_calendar(api, today-timedelta(1), today+timedelta(3))
        REST_DAYS = list(trading_calendar[trading_calendar["trading"] == False].date.map(lambda x:x.date()))
    elif (today+timedelta(7)).year > REST_DAYS[-1].year:
        from tqsdk import calendar
        calendar._init_chinese_rest_days() #更新假期,3.03版本使用
        REST_DAYS = list(calendar.rest_days_df.date.map(lambda x:x.date())) #3.03版本使用
    rest_today = today.date() in REST_DAYS #今日是否假日
    rest_tomorrow = (today+timedelta(1)).date() in REST_DAYS #明日是否假日
    rest_yestoday = (today-timedelta(1)).date() in REST_DAYS #昨日是否假日
    rest_friday = today.weekday() == 4 and (today+timedelta(3)).date() in REST_DAYS  #周一假日
    rest_saturday = today.weekday() == 5 and (today+timedelta(2)).date() in REST_DAYS #周一假日
    trading = False
    #白盘夜盘周一至周五,凌晨周二至周六,剔除假日,假日前夜盘休市
    if not rest_today and ((day_start <= today.time() <= day_end  or not (rest_tomorrow or rest_friday) and night_start <= today.time()) 
        and today.weekday() <= 4 or not (rest_yestoday or rest_saturday) and (today.time() <= night_end and 0 < today.weekday() <= 5)):
        trading = True
    return trading #交易时段

def SafeDivide(a,b):
    if b != 0: return a / b
    else: return 0

def _str_vs_float(_pos_dict,_Position_Log):
    #保存本地文件
    _Position_Log.seek(0) #
    _Position_Log.truncate() #
    json.dump(_pos_dict, _Position_Log,indent=4) 
    _Position_Log.flush() #

def exp_d(symbole_info):
    expd = f"到期日:{symbole_info.ExpireDate},到期剩余日:{symbole_info.expire_rest_days}\n"+f"交割月前一月最后交易日:{symbole_info.last_day_pre_expire_month },距最后交易日剩余日:{symbole_info.pre_expire_days}\n"
    
    return expd

#策略主体
def cta(pqapi:PeopleQuantApi,pos_lock,symbol,account,DingChan,_Position_Log,_pos_dict,**kw ):
    quote = pqapi.get_quote(symbol)        #获取合约行情
    symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
    position = pqapi.get_position(symbol)   #获取合约持仓
    UpdateTime = quote.ctp_datetime #行情更新时间
    PriceTick = symbole_info.PriceTick
    commission_tick = 10/symbole_info.VolumeMultiple/PriceTick #每手手续费计算跳数,根据合约属性计算,也可固定值,如10元/手
    balance = kw["balance"] if "balance" in kw else 0  #账户最低权益
    risk_ratio = kw["risk_ratio"] if "risk_ratio" in kw else 1  #账户风险度
    OrderMemo = 'pqapi' #报单备注,便于区分其他来源报单,如手工单等,也可不填
    #日撤单数、分钟撤单数、日报单数、日开仓数、最大开仓数
    minute_cancels,day_orders = 10,500 #回报数据丢失情况下的本地报单阈值防范
    day_cancels,minute_cancels,day_orders,daylotss,maxlotss = 100,10,500,20000,20000  #阈值
    day_cancel,minute_cancel,day_order,che_time = 0,0,0,datetime.now().timestamp() #初始值
    #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量       阈值
    orders_insert,orders_cancel,daylots,self_trade,order_exe = 500,200,1000,2,3000
    while True:
        #只监控本客户端报单,如有手工单等其他来源报单,可在报单备注OrderMemo里区分,如不填则监控所有报单
        orders = pqapi.get_symbol_order(InstrumentID=symbol,OrderStatus='Alive',OrderMemo=OrderMemo,_print=False)
        #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
        orderrisk = pqapi.get_order_risk(symbol)
        for order in orders:
            r = pqapi.check_order(order)
            pqapi.send_message(DingChan, account, orderrisk, r,name="test",e="")  #发邮箱提醒
            if r['shoushu']:
                with pos_lock:
                    _pos_dict[symbol].update({"open_price_long":position.open_price_long,"open_price_short":position.open_price_short,"pos_long":position.pos_long,"pos_short":position.pos_short})
                    if not position.pos_short:
                        _pos_dict[symbol].update({"highest_profit":0,"protect_profit":False})
                _str_vs_float(_pos_dict,_Position_Log)
            #print('方向:',r['kaiping'],'成交手数',r['shoushu'],'成交均价',r['junjia'],'报单信息',r["last_msg"] )
        if UpdateTime != quote.ctp_datetime: #新行情推送
            UpdateTime = quote.ctp_datetime
            #权益足够,风险度足够,否则只平不开
            risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
            #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量
            orderrisk = pqapi.get_order_risk(symbol)
            order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and
                            orderrisk["open_volume"] < daylots and orderrisk["self_trade_count"] < self_trade and
                            orderrisk["order_exe"] < order_exe)
            
            if position.pos_short :
                profit_price = quote.AskPrice1 if quote.LowerLimitPrice <= quote.AskPrice1 <= quote.UpperLimitPrice else float('nan')  #+ PriceTick
                if _pos_dict[symbol]["highest_profit"] >= position.open_price_short * 0.5: 
                    with pos_lock:_pos_dict[symbol]['protect_profit'] = True  #达到最高利润一半,可以考虑浮盈加仓
                if position.open_price_short - profit_price > _pos_dict[symbol]["highest_profit"]:
                    with pos_lock:_pos_dict[symbol]["highest_profit"] = position.open_price_short - profit_price
                    _str_vs_float(_pos_dict,_Position_Log)
            if order_enable:
                profit_price = quote.BidPrice1 + PriceTick
                if (position.pos_short and _pos_dict[symbol]["highest_profit"] >= position.open_price_short*0.5
                    #归还权利金超过最高利润的一半
                    and position.open_price_short - commission_tick*2  > profit_price >= position.open_price_short - _pos_dict[symbol]["highest_profit"]*0.5):
                    price = profit_price
                    open_price_short = position.open_price_short  #初始开仓价位
                    r = pqapi.open_close(symbol,"pingkong",position.pos_short,price,order_info='平仓',OrderMemo=OrderMemo)
                    pqapi.send_message(DingChan, account, orderrisk, r,name="test",e="")  #发邮箱提醒
                    #print(r['kaiping'],r['shoushu'],r['junjia'],r["last_msg"] )
                    if r['shoushu']:
                        #profit_count = open_price_short - r['junjia']  #盈利价差
                        #profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                        with pos_lock:
                            _pos_dict[symbol].update({"open_price_long":position.open_price_long,"open_price_short":position.open_price_short,"pos_long":position.pos_long,"pos_short":position.pos_short})
                            if not position.pos_short:
                                _pos_dict[symbol].update({"highest_profit":0,"protect_profit":False})
                        _str_vs_float(_pos_dict,_Position_Log)
                    else:
                        if r['order_wrong']: #下单错误
                            print(r['last_msg']) #错误信息
                            return
            else: #权益、风险度、报撤单等超过阈值
                print(account,orderrisk)    
                pqapi.send_message(DingChan, account, orderrisk, name="test",e="权益、风险度、报撤单等超过阈值")  #发邮箱提醒
                break #退出循环结束策略
        tm.sleep(0.001)

#策略子进程
def Cta(ZhangHu=[],s='',error_queue=None,**kw):  
    try:
        global REST_DAYS, day_start, day_end, night_start, night_end
        DingDing = kw["DingDing"] if "DingDing" in kw else []
        WeChat = kw["WeChat"] if "WeChat" in kw else []
        QQemail = kw["QQemail"] if "QQemail" in kw else []
        DingChan = ""

        TradeFrontAddr = ZhangHu["TradeFrontAddr"]   #交易前置地址
        MdFrontAddr = ZhangHu["MdFrontAddr"]     #行情前置地址

        BROKERID = ZhangHu['BROKERID']   #期货公司ID
        USERID = ZhangHu['USERID']   #账户
        PASSWORD = ZhangHu['PASSWORD']   #登录密码
        APPID = ZhangHu['APPID']   #客户端ID
        AUTHCODE = ZhangHu['AUTHCODE']  #授权码
        _flog = r'C:\AppData\Roaming\pqapilogs'  #程序所在目录下创建logs目录
        os.makedirs(_flog,exist_ok=True)
        if os.path.isfile(_flog+fr'\PositionLog{s}.json') != True: #
            with open(_flog+fr'\PositionLog{s}.json','w+',encoding='utf-8') as _Position_Log:
                json.dump({},_Position_Log)  
            _Position_Log=open(_flog+fr'\PositionLog{s}.json','r+',encoding='utf-8') 
        else : _Position_Log=open(_flog+fr'\PositionLog{s}.json','r+',encoding='utf-8') #
        _pos_dict: dict = json.load(_Position_Log)    #
        if "profit" not in _pos_dict: _pos_dict["profit"] = 0
        #创建api实例
        pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=s,flowfile="")
        #loop = asyncio.SelectorEventLoop() #创建事件循环
        #asyncio.set_event_loop(loop)
        #api = TqApi( auth=TqAuth('',''),loop=loop, debug=False)
        account = pqapi.get_account()  #获取账户资金
        positions = pqapi.get_position() #获取账户持仓
        DingChan = ""
        content = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n账户权益:{round(account.Balance,2)},可用资金:{round(account.Available,2)},持仓浮盈:{round(account.float_profit,2)},持仓保证金:{round(account.CurrMargin,2)},风险度:{round(account.risk_ratio,2)},市值权益:{round(account.market_balance,2)},市值风险度:{round(account['market_risk_ratio'],2)}\n账户登录"
        if (DingDing or QQemail or WeChat):
            DingChan = zhuchannel.ThreadChan()
            send_msg_task = zhuchannel.WorkThread(pqapi.send_msg,args=(DingDing,WeChat,QQemail, DingChan ),kwargs={"logfile":pqapi._logfile,"_print":False, "sf": ""})
            send_msg_task.start()
        if isinstance(DingChan,zhuchannel.ThreadChan): DingChan.put_nowait(content)
        cta_thread = set()
        pos_lock = threading.Lock() #持仓记录锁
        for symbol,p in positions.items():
            if p.ins_class in ["CALL","PUT"]:
                if symbol not in _pos_dict:
                    _pos_dict[symbol] = {"open_price_long":p.open_price_long,"open_price_short":p.open_price_short,"pos_long":p.pos_long,"pos_short":p.pos_short,
                                        "highest_profit":0,"protect_profit":False}
                else:
                    _pos_dict[symbol].update({"open_price_long":p.open_price_long,"open_price_short":p.open_price_short,"pos_long":p.pos_long,"pos_short":p.pos_short})
                #创建策略任务线程
                cta1 = zhuchannel.WorkThread(cta,args=(pqapi,pos_lock,symbol, account, DingChan,_Position_Log,_pos_dict),kwargs={},daemon=True)
                cta1.start()
                cta_thread.add(symbol)
        #print(_pos_dict)
        notify_time = 0 #通知时间
        #循环判断是否处于交易时间，非交易时间关闭api
        while True:
            if "balance" in kw and account.Balance <= kw["balance"]:
                if datetime.now().timestamp() - notify_time >= 5*60: #每5分钟通知一次
                    content = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n风险警告,账户权益触及设定值:{kw['balance']},当前权益:{account.Balance}"
                    print(content)
                    pqapi.send_message(DingChan, account, name="test",e=content)  #发邮箱提醒
                    if isinstance(DingChan,zhuchannel.ThreadChan):
                        while not DingChan.empty():tm.sleep(5)
                    notify_time = datetime.now().timestamp() #通知时间
            
            for symbol,p in positions.items():
                if p.ins_class in ["CALL","PUT"]:
                    if symbol not in _pos_dict:
                        with pos_lock: #持仓记录更新
                            _pos_dict[symbol] = {"open_price_long":p.open_price_long,"open_price_short":p.open_price_short,"pos_long":p.pos_long,"pos_short":p.pos_short,
                                                "highest_profit":0,"protect_profit":False}
                        _str_vs_float(_pos_dict,_Position_Log)
                    #仓位有更新
                    elif  p.open_price_short != _pos_dict[symbol]["open_price_short"] or p.pos_short != _pos_dict[symbol]["pos_short"]:
                        with pos_lock: #持仓记录更新
                            _pos_dict[symbol].update({"open_price_long":p.open_price_long,"open_price_short":p.open_price_short,"pos_long":p.pos_long,"pos_short":p.pos_short})
                        _str_vs_float(_pos_dict,_Position_Log)
                    if symbol not in cta_thread: #新持仓创建策略线程
                        #创建策略任务线程
                        cta1 = zhuchannel.WorkThread(cta,args=(pqapi,pos_lock,symbol, account, DingChan,_Position_Log,_pos_dict),kwargs={},daemon=True)
                        cta_thread.add(symbol)
                        cta1.start()
            t = datetime.today().time()
            if day_end < t < night_start or night_end < t < day_start: #非交易时间关闭api
                if isinstance(DingChan,zhuchannel.ThreadChan):
                    sp = ""
                    ep = "持仓明细:\n"
                    for symbol, p in positions.items():
                        symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
                        quote = pqapi.get_quote(symbol)
                        expd = exp_d(symbole_info)
                        if p.pos_long :
                            ep += f"{p.instrument_id}-{expd}多单-手数:{p.pos_long},价格:{round(p.open_price_long,quote.price_decs)},浮盈:{round(p.float_profit_long,2)},保证金:{round(p.margin_long,2)},保证金收益率:{round(SafeDivide(p.float_profit_long,p.margin_long),2)}\n"
                        if p.pos_short :
                            ep += f"{p.instrument_id}-{expd}空单-手数:{p.pos_short},价格:{round(p.open_price_short,quote.price_decs)},浮盈:{round(p.float_profit_short,2)},保证金:{round(p.margin_short,2)},保证金收益率:{round(SafeDivide(p.float_profit_short,p.margin_short),2)}\n"
                        else:
                            if symbol in _pos_dict and not _pos_dict[symbol]['pos_short']: _pos_dict.pop(symbol) #清除无持仓记录
                    for symbol in list(_pos_dict.keys()):
                        if symbol in ['profit']: continue
                        if symbol not in positions : #清除已平仓记录
                            _pos_dict.pop(symbol)
                    sp += ep  
                    content = "{}{}{}{}".format(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n算法累计盈利:{round(_pos_dict['profit'],2)}\n\n账户今日盈亏:{round(account.Balance-account.static_balance,2)},",
                                                f"账户今日收益率:{round(SafeDivide(account.Balance-(account.static_balance-account.PreBalance),account.PreBalance),2)}\n\n账户持仓统计:\n账户权益:{round(account.Balance,2)},",
                                                f"可用资金:{round(account.Available,2)},持仓浮盈:{round(account.float_profit,2)},持仓保证金:{round(account.CurrMargin,2)},保证金收益率:{round(SafeDivide(account.float_profit,account.CurrMargin),2)},手续费:{account.Commission},风险度:{round(account.risk_ratio,2)},",
                                                f"市值权益:{round(account.market_balance,2)},市值风险度:{round(account['market_risk_ratio'],2)}\n\n{sp}\n账户退出")
                    ln = len(content)
                    lln = 2000 if "msg" not in  kw else kw["msg"]
                    num = int(ln/lln) + 1 if ln/lln - int(ln/lln) > 0 else int(ln/lln)
                    for i in range(1,num+1):
                        DingChan.put_nowait(f"第{i}部分,共{num}部分\n\n" + content[(i-1)*lln:lln*i])
                _str_vs_float(_pos_dict,_Position_Log )
                while isinstance(DingChan,zhuchannel.ThreadChan) and not DingChan.empty():tm.sleep(5)
                print('非交易时间,退出账户',datetime.today())
                break #退出循环结束进程
            tm.sleep(0.001)
    
    except : #异常保存在本地，便于事后检查
        #非超时异常
        _str_vs_float(_pos_dict,_Position_Log)
        e = traceback.format_exc()
        pqapi.logs_txt(e,pqapi._logfile)
        if error_queue is not None: error_queue.put(True)
        if isinstance(DingChan,zhuchannel.ThreadChan): 
            ln = len(e)
            lln = 2000 if "msg" not in  kw else kw["msg"]
            num = int(ln/lln) + 1 if ln/lln - int(ln/lln) > 0 else int(ln/lln)
            for i in range(1,num+1):
                DingChan.put_nowait(f"第{i}部分,共{num}部分\n\n" + e[(i-1)*lln:lln*i])
            while not DingChan.empty():tm.sleep(5)
        #_str_vs_float(_pos_dict,_Position_Log)
    else:
        if error_queue is not None: 
            error_queue.put(False)     
            print('通知主进程账户退出',datetime.today())
    finally:
        pass
#主进程
def Main(ZhangHu=[],s="_all",**kw ):
    child_process = None
    #接收策略异常
    error_queue = multiprocessing.Queue()
    cta_error = False
    while True:
        trading = Trading_time( )        
        #交易时段启动策略 #子进程崩溃时也可重开
        if not cta_error and trading and (child_process == None or isinstance(child_process,multiprocessing.Process) and not child_process.is_alive()):
            print("启动账户",datetime.today())
            child_process = multiprocessing.Process(target=Cta,args=(ZhangHu,s,error_queue,),kwargs=kw)
            child_process.start()
            #cta_error = error_queue.get()
            #child_process.join(30)
            #if cta_error: print("策略异常,账户退出",datetime.today())
            #else: print("策略子进程退出",datetime.today())
        #非交易时段退出子进程
        if not trading and child_process is not None:
            if not child_process.is_alive(): #子进程已结束
                child_process = None
                print("账户关闭成功",datetime.today())
                
            elif child_process.is_alive():
                child_process.terminate() #子进程被阻塞，强制关闭进程，api未关闭不会报错
                child_process.join()
                child_process = None
                print("账户强制关闭",datetime.today())                
        if cta_error: 
            if child_process.is_alive(): 
                child_process.terminate()  # 终止子进程
                child_process.join()
            print("程序异常退出",datetime.today())
            return    # 主进程退出
        sleep(300) #5分钟检查一次


if __name__ == "__main__":
    #DingDing = ["宏源期权移动止盈","https://oapi.dingtalk.com/robot/send?accessc95f26c07c4ca9ddec"]   
    #QQemail = ["123576@qq.com","sgh323","123576@qq.com","算法通知"]  #发QQ邮箱
    QQemail =[]# ["3416223155@qq.com","hgiutyargkfnchdd","3416223155@qq.com","test测试"]
    #WeChat = ["模拟测试","https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=4ba50e86-aee6-4025-9723-98f8f59bc8f4"]

    #设置期货户账户,例如 ZhangHu = ["G国泰君安","期货账号","期货密码"] 为实盘模式 ,括号里不填则为模拟或回测模式
    #模拟交易或回测
    ZhangHu = {"BROKERID":"H宏源期货_主席","USERID":"","PASSWORD":"",
               "APPID":"","AUTHCODE":"",
                "TradeFrontAddr":"",
                "MdFrontAddr":""}
    #多点运行,区分不同文件
    s = "hyqq" 

    Main(ZhangHu=ZhangHu,s=s,DingDing=[],WeChat=[],QQemail=QQemail)