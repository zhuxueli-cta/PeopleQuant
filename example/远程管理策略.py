#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
from pathlib import Path
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
root_path = os.path.abspath(os.path.join(current_path, "../../../"))
# 将根目录添加到 sys.path
if root_path not in sys.path:
    sys.path.insert(0,root_path)
import time as tm
import asyncio
import traceback
import polars
from datetime import datetime,time,date,timedelta
import copy
from typing import Dict, List, Optional, Tuple, Any
import secrets
import socket
import requests
from collections import defaultdict
import threading
import multiprocessing
# 在推送数据前，清理非法值（以处理 NaN/Infinity 为例）
import json
from decimal import Decimal
# 本地缓存（仅 WebSocket 进程可见）
local_update_cache = defaultdict(list)
# 缓存锁（避免多线程竞争）
cache_lock = threading.Lock()
def clean_data(data):
    """递归清理数据，确保 100% JSON 可序列化"""
    if isinstance(data, dict):
        return {k: clean_data(v) for k, v in data.items() if v is not None}  # 过滤 None 值
    elif isinstance(data, list):
        return [clean_data(v) for v in data if v is not None]
    elif isinstance(data, float):
        # 处理 NaN、Infinity，强制转为合法数字
        if not isinstance(data, float) or data != data or data in (float('inf'), float('-inf')):
            return 0.0
        return round(data, 2)
    elif isinstance(data, (int, str, bool)):
        return data  # 合法类型直接返回
    else:
        return str(data) if data is not None else ""  # 其他类型转为字符串（避免序列化失败）

def run_websocket_server(strategies,update_queue,command_queue):
    """独立进程运行 WebSocket 服务"""
    from flask import Flask, render_template_string
    from flask_socketio import SocketIO, emit
    # 第一步：导入 gevent 并执行猴子补丁（仅替换必要模块，不干扰 CTP）
    import gevent
    from gevent import monkey
    # 关键：只补丁 socket 和 select（WebSocket 必需），跳过 thread/threading（避免干扰 CTP 线程）
    monkey.patch_socket()
    monkey.patch_select()
    # 明确禁用线程补丁（关键！避免影响 CTP 线程）
    monkey.patch_thread = False  # 禁止补丁 threading 模块
    monkey.patch_os = False      # 可选：避免补丁 OS 相关函数
    monkey.patch_subprocess = False  # 可选：如果 CTP 用到 subprocess
    # 仅为 WebSocket 补丁必要模块，不影响 CTP 进程
    monkey.patch_threading = False
    

    app = Flask(__name__)
    # 生成 32 字符的随机字符串（包含字母、数字、特殊符号）
    secrets.token_hex(16) # 16字节=32字符，结果类似：'a1b2c3d4e5f67890a1b2c3d4e5f67890'
    # 或生成更复杂的字符串（包含特殊符号）
    # 结果类似：'z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXc'
    app.config['SECRET_KEY'] = secrets.token_urlsafe(24)  # 替换为随机字符串
    socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='gevent',
    ping_timeout=120,
    ping_interval=45
    )
    def get_local_ip():
        """获取本地局域网IP"""
        try:
            # 连接外部服务器获取本地IP（不实际建立连接）
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('8.8.8.8', 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except:
            return '127.0.0.1'  # 异常时返回本地回环地址

    def get_public_ip():
        """获取公网IP（依赖外部接口，需联网）"""
        try:
            # 调用免费公网IP查询接口
            response = requests.get('https://api.ipify.org?format=json', timeout=5)
            return response.json()['ip']
        except:
            try:
                response = requests.get('https://ifconfig.me/ip', timeout=5)
                return response.text.strip()
            except:
                return '无法获取公网IP（请检查网络）'

    # 定时推送更新（根据needs_update标记）
    def batch_push_updates():
        while True:
            try:
                # 收集需要更新的策略
                updates = []
                while not update_queue.empty():
                    with cache_lock:
                        cach_strategies = copy.deepcopy(strategies)
                        strategies.update(update_queue.get())
                        
                        if len(strategies):  # 检查是否为空
                            for strategy_id in strategies:
                                strategy = strategies[strategy_id]
                                if strategy_id in cach_strategies:
                                    for contract in cach_strategies[strategy_id]['contracts']:
                                        for c in strategy['contracts']:
                                            if contract['id'] == c['id']:
                                                c['status'] = contract['status']
                                                break
                                cleaned = clean_data(strategy)
                                updates.append(cleaned)
                                
                if updates:
                    socketio.emit('batch_strategy_updates', updates)
            except Exception as e:
                print(f"批量推送异常: {e}")
            gevent.sleep(1)

    # 初始数据推送
    @socketio.on('request_initial_data')
    def handle_initial_data():
        if not strategies:  # 检查是否为空
            return
        data = clean_data(list(strategies.values()))
        socketio.emit('initial_data', data)
        print(f"初始推送完成:{data}")

    # 处理前端状态更新请求
    # 处理前端状态更新请求
    @socketio.on('update_strategy_status')
    def handle_strategy_status(data):
        try:
            strategy_id = data["strategy_id"]
            target_status = data["status"]
            with cache_lock:
                if strategy_id in strategies:
                    # 更新策略状态
                    for contract in strategies[strategy_id]["contracts"]:
                        contract["status"] = target_status
                    # 关键：非阻塞放入队列，避免满队列导致的阻塞
                    try:
                        command_queue.put_nowait(copy.deepcopy(strategies))
                        #print(f"策略推送: strategy_id {strategy_id} {strategies}→ {target_status}")  # 确保日志正常输出
                    except :
                        print("命令队列已满，跳过本次推送")  # 队列满时不阻塞
        except Exception as e:
            print(f"策略状态处理异常: {e}")  # 捕获异常，避免进程崩溃
    # 修改状态处理函数，触发更新时直接调用推送
    @socketio.on('update_contract_status')
    def handle_contract_status(data):
        try:
            strategy_id = data["strategy_id"]
            contract_id = data["contract_id"]
            target_status = data["status"]
            with cache_lock:
                if strategy_id in strategies:
                    for contract in strategies[strategy_id]["contracts"]:
                        if contract["id"] == contract_id:
                            contract["status"] = target_status
                            # 关键：非阻塞放入队列
                            try:
                                command_queue.put_nowait(copy.deepcopy(strategies))
                                #print(f"合约推送: contract_id{contract_id} {strategies} → {target_status}")
                            except :
                                print("命令队列已满，跳过本次推送")
                            break
        except Exception as e:
            print(f"合约状态处理异常: {e}")
    # 路由
    @app.route('/trade')
    def trade_interface():
        # 获取主程序所在目录的绝对路径
        current_dir = Path(__file__).resolve().parent
        template_path = current_dir / "trade_template.html"  # 拼接模板路径（跨平台兼容）
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
        return render_template_string(template_content)
    
    # 启动同步线程
    gevent.spawn(batch_push_updates)
    #threading.Thread(target=batch_push_updates).start()

    # 1. 获取并打印连接地址
    local_ip = get_local_ip()
    public_ip = get_public_ip()
    port = 8080
    print("="*50)
    print("服务器已启动，可通过以下地址访问：")
    print(f"本地访问：http://{local_ip}:{port}/trade")
    print(f"本地回环：http://127.0.0.1:{port}/trade")
    print(f"公网访问：http://{public_ip}:{port}/trade")
    print("="*50)
    print("提示：公网访问需确保服务器端口8080已开放防火墙，且有公网IP")
    # 用 eventlet 启动服务器（替代 socketio.run 的默认模式）
    socketio.run(
        app,
        host='0.0.0.0',
        port=8080,
        debug=False,  # 生产环境关闭调试模式
        use_reloader=False,  # 关闭自动重载
        #log_output=False,  # 关闭冗余日志，避免干扰
        log_output=True  # 打印 SocketIO 连接日志
    )


def run_ctp_service(strategies,update_queue,command_queue):
    
    from peoplequant.pqctp import PeopleQuantApi 
    from peoplequant import zhuchannel

    """主进程：CTP 行情处理+写入更新队列"""
    
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

    def comand_status(strategies):
        while True:
            try:
                cach_strategies = command_queue.get()
                strategies.update(cach_strategies)
            except :
                pass       

    def cta(pqapi:PeopleQuantApi,strategies,name,account,symbol,update_queue,command_queue,filename,logfile,**kw ):
        quote = pqapi.get_quote(symbol)        #获取合约行情
        symbole_info = pqapi.get_symbol_info(symbol) #获取合约属性
        position = pqapi.get_position(symbol)   #获取合约持仓
        UpdateTime = quote.ctp_datetime #行情更新时间
        localtime = quote.local_timestamp
        PriceTick = symbole_info['PriceTick']
        lot = 5 #下单手数
        balance = kw["balance"] if "balance" in kw else 0  #账户最低权益
        risk_ratio = kw["risk_ratio"] if "risk_ratio" in kw else 1  #账户风险度
        #报单次数、撤单次数、多空开仓成交总手数、自成交数、信息量       阈值
        orders_insert,orders_cancel,daylots,self_trade,order_exe = 50000,50000,100000,10000,100000
        target_contract,target_strategy = None,None
        while True:
            t = tm.time()
            for strategy_id in strategies:
                strategy = strategies[strategy_id]
                if strategy["name"] == name:  # 匹配当前策略
                    for contract in strategy["contracts"]:
                        if contract["name"] == symbol:  # 匹配当前合约
                            target_contract = contract  # 缓存共享引用
                            target_strategy = strategy
                            break
                    if target_contract:
                        break
            if target_contract["status"] == "pause"  :
                tm.sleep(1)
                continue
            if UpdateTime != quote.ctp_datetime and localtime != quote.local_timestamp: #新行情推送
                UpdateTime = quote.ctp_datetime
                localtime = quote.local_timestamp
                print((quote.InstrumentID,quote.LastPrice,quote.Volume,target_contract["status"]))
                #权益足够,风险度足够,否则只平不开
                risk_control = account["Balance"] > balance and account["risk_ratio"] < risk_ratio
                orderrisk = pqapi.get_order_risk(symbol)
                order_enable = (orderrisk["order_count"] < orders_insert and orderrisk["cancel_count"] < orders_cancel and
                                orderrisk["open_volume"] < daylots and orderrisk["self_trade_count"] < self_trade and
                                orderrisk["order_exe"] < order_exe)
                buy_up = 1  #多头信号
                sell_down = 1  #空头信号
                if order_enable:
                    if buy_up and not position.pos_long and risk_control:
                        price = quote["AskPrice1"]
                        r = pqapi.open_close(symbol,"kaiduo",lot,price,order_info='开仓')
                        if r['shoushu']:
                            new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':0,'利润金额':0, '日期':f'{UpdateTime}'}])
                            pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                            #print(account)
                    elif sell_down and not position.pos_short and risk_control:
                        price = quote["BidPrice1"]
                        r = pqapi.open_close(symbol,"kaikong",lot,price,order_info='开仓')
                        if r['shoushu']:
                            new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':0,'利润金额':0, '日期':f'{UpdateTime}'}])
                            pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                            #print(account)
                    if position.pos_long and abs(quote.LastPrice - position.open_price_long) >= 5*PriceTick:
                        price = quote["BidPrice1"]
                        open_price_long = position.open_price_long  #初始开仓价位
                        r = pqapi.open_close(symbol,"pingduo",position.pos_long,price,order_info='平仓')
                        if r['shoushu']:
                            profit_count = r['junjia'] - open_price_long #盈利价差
                            profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                            new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':profit_count,'利润金额':profit_money, '日期':f'{UpdateTime}'}])
                            pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                            #print(account)
                    if position.pos_short and abs(quote.LastPrice - position.open_price_short) >= 5*PriceTick:
                        price = quote["AskPrice1"]
                        open_price_short = position.open_price_short  #初始开仓价位
                        r = pqapi.open_close(symbol,"pingkong",position.pos_short,price,order_info='平仓')
                        if r['shoushu']:
                            profit_count = open_price_short - r['junjia']  #盈利价差
                            profit_money = profit_count * r['shoushu'] * symbole_info["VolumeMultiple"] #盈利金额
                            new_df = polars.DataFrame([{'策略':name,'品种':symbole_info['ProductID'],'合约':symbol,'方向':r['kaiping'],'下单价格':r['price'],'下单数量':r['lot'],'成交价格':r['junjia'],'成交数量':r['shoushu'],'盘口挂单量':r['quote_volume'],'备注':r['order_info'],'利润点数':profit_count,'利润金额':profit_money, '日期':f'{UpdateTime}'}])
                            pqapi.trade_excel(new_df,'交易统计',filename,logfile) #保存全部成交记录
                            #print(account)
                if target_contract["latest_price"] != quote.LastPrice and target_contract["status"] == 'enable' :
                    # 推送该策略的完整数据（仅包含变化的策略）
                    target_contract["pos_long"] = position.pos_long
                    target_contract["pos_short"] = position.pos_short
                    target_contract["open_price_long"] = position.open_price_long
                    target_contract["open_price_short"] = position.open_price_short
                    target_contract["float_profit_long"] = position.float_profit_long
                    target_contract["float_profit_short"] = position.float_profit_short
                    target_contract["latest_price_change"] = quote.LastPrice - target_contract["latest_price"]
                    target_contract["latest_price"] = quote.LastPrice
                    target_strategy["needs_update"] = True
                    update_queue.put_nowait(strategies)
                    
            tm.sleep(1)
    
    #创建api实例
    pqapi = PeopleQuantApi(BrokerID=BROKERID, UserID=USERID, PassWord=PASSWORD, AppID=APPID, AuthCode=AUTHCODE, TradeFrontAddr=TradeFrontAddr, MdFrontAddr=MdFrontAddr, s=USERID,flowfile="")
    #获取账户资金
    account = pqapi.get_account()            
    positions = pqapi.get_position()
    

    cta1 = zhuchannel.WorkThread(cta,args=(pqapi,strategies,'策略1',account,'hc2605',update_queue,command_queue,'策略1',r'C:\CTPLogs' ),kwargs={})
    cta2 = zhuchannel.WorkThread(cta,args=(pqapi,strategies,'策略2',account,'rb2605',update_queue,command_queue,'策略2',r'C:\CTPLogs' ),kwargs={})
    cta3 = zhuchannel.WorkThread(cta,args=(pqapi,strategies,'策略3',account,'m2605',update_queue, command_queue,'策略3',r'C:\CTPLogs'),kwargs={})
    cta4 = zhuchannel.WorkThread(cta,args=(pqapi,strategies,'策略1',account,'y2605',update_queue, command_queue,'策略1',r'C:\CTPLogs'),kwargs={})
    cta1.start()
    cta2.start()
    cta3.start()
    cta4.start()
    zhuchannel.WorkThread(comand_status,args=(strategies,),kwargs={}).start()


# 启动批量推送线程（daemon=True 随程序退出）
#threading.Thread(target=batch_push_updates, daemon=True).start()

if __name__ == '__main__':
    # 新增反向队列：WebSocket 进程→主进程（处理状态更新）
    command_queue = multiprocessing.Queue(maxsize=100)
    # 全局队列：主进程（CTP）将持仓更新放入队列，WebSocket 进程从队列读取
    update_queue = multiprocessing.Queue(maxsize=1000)  # 限制队列大小，避免内存溢出

    # -------------------------- 后端逻辑 --------------------------
    # 全局状态：存储策略和合约数据（实际应用中可替换为数据库）
    # 数据结构：区分多头/空头独立盈亏
    ctas_info = {"hc2605":"策略1","rb2605":"策略2","m2605":"策略3","y2605":"策略1",}
    strategies = {}
    # 1. 初始化策略和映射（启动时执行一次）
    # 临时字典：key=策略名，value=该策略下的合约名列表
    strategy_contracts = defaultdict(list)
    for contract_name, strategy_name in ctas_info.items():
        strategy_contracts[strategy_name].append(contract_name)
    # 遍历分组结果，生成完整的 strategies 结构
    for idx, (strategy_name, contract_names) in enumerate(strategy_contracts.items(), 1):
        # 策略ID：s1、s2、s3...（按顺序生成）
        strategy_id = f"s{idx}"
        # 为每个合约生成详细信息（初始值可根据实际情况调整）
        contracts = []
        for c_idx, contract_name in enumerate(contract_names, 1):
            # 合约ID：c1、c2...（按策略内顺序生成）
            contract_id = f"{strategy_id}_{c_idx}"  # 全局唯一
            contracts.append({
                "id": contract_id,
                "name": contract_name,
                "pos_long": 0,
                "open_price_long": 0,
                "float_profit_long": 0,
                "pos_short": 0,
                "open_price_short": 0,
                "float_profit_short": 0,
                "latest_price": 0,
                "latest_price_change": 0,
                "status": "enable"  # 初始状态：启用
            })
        # 将策略添加到 strategies 字典
        strategies[strategy_id] = {
            "id": strategy_id,
            "name": strategy_name,
            "contracts": contracts,
            "needs_update": False  # 初始无需更新
        }

    # 启动 WebSocket 独立进程
    websocket_process = multiprocessing.Process(target=run_websocket_server,args=(strategies, update_queue,command_queue))
    #websocket_process.daemon = True  # 主进程退出时自动终止
    websocket_process.start()
    # 主进程运行 CTP 服务（传入队列）
    run_ctp_service(strategies, update_queue,command_queue)
    
