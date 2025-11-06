#!usr/bin/env python3
#-*- coding:utf-8 -*-
import time as tm
import multiprocessing.queues
from typing import Any, List, TYPE_CHECKING, Union
import queue 
import multiprocessing 
import threading
import sys,asyncio,os
import collections
import traceback
import copy
from datetime import datetime,time,date

# 全局线程注册表:记录所有启动的WorkThread实例
WORK_THREAD_REGISTRY: List["WorkThread"] = []
# 线程安全锁:操作注册表时避免竞争
REGISTRY_LOCK = threading.Lock()

#日志            
def logs_txt(e,logfile):
    print(e)
    os.makedirs(logfile,exist_ok=True)
    ss = logfile+f"\\{date.today()}.txt"
    ff = open(ss,mode="a+",encoding='utf-8')
    ff.write("\n"+datetime.today().isoformat(" ")+"-"*30+"\n")
    ff.write(e)
    ff.close()

class TqChan(asyncio.Queue):

    def __init__(self, loop, last_only: bool = False, chan_name: str = "") -> None:
        """
        创建channel实例
        Args:
            last_only (bool): 为True时只存储最后一个发送到channel的对象
        """
        #loop = asyncio.SelectorEventLoop()
        #asyncio.set_event_loop(loop)
        py_ver = sys.version_info
        asyncio.Queue.__init__(self, loop=loop) if (py_ver.major == 3 and py_ver.minor < 10) else asyncio.Queue.__init__(self)
        self._last_only = last_only
        self._closed = False

    async def close(self) -> None:
        """
        关闭channel
        关闭后send将不起作用,recv在收完剩余数据后会立即返回None
        """
        if not self._closed:
            self._closed = True
            await asyncio.Queue.put(self, None)

    async def send(self, item: Any) -> None:
        """
        异步发送数据到channel中
        Args:
            item (any): 待发送的对象
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    asyncio.Queue.get_nowait(self)
            await asyncio.Queue.put(self, item)

    def send_nowait(self, item: Any) -> None:
        """
        尝试立即发送数据到channel中
        Args:
            item (any): 待发送的对象
        Raises:
            asyncio.QueueFull: 如果channel已满则会抛出 asyncio.QueueFull
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    asyncio.Queue.get_nowait(self)
            asyncio.Queue.put_nowait(self, item)

    async def recv(self) -> Any:
        """
        异步接收channel中的数据，如果channel中没有数据则一直等待
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        """
        if self._closed and self.empty():
            return None
        item = await asyncio.Queue.get(self)
        return item

    def recv_nowait(self) -> Any:
        """
        尝试立即接收channel中的数据
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        Raises:
            asyncio.QueueFull: 如果channel中没有数据则会抛出 asyncio.QueueEmpty
        """
        if self._closed and self.empty():
            return None
        item = asyncio.Queue.get_nowait(self)
        return item

    def recv_latest(self, latest: Any) -> Any:
        """
        尝试立即接收channel中的最后一个数据
        Args:
            latest (any): 如果当前channel中没有数据或已关闭则返回该对象
        Returns:
            any: channel中的最后一个数据
        """
        while (self._closed and self.qsize() > 1) or (not self._closed and not self.empty()):
            latest = asyncio.Queue.get_nowait(self)
        return latest

    def clear(self):
        """
        清空channel中的数据
        """
        while not self.empty():
            asyncio.Queue.get_nowait(self)
        #self._FTDdeque.clear()
        #self._FTDdeque.append(0)  #请求时间重置

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await asyncio.Queue.get(self)
        if self._closed and self.empty():
            raise StopAsyncIteration
        return value

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

class ThreadChan(queue.Queue):

    def __init__(self, last_only: bool = False, maxsize: int = 0, maxlen: int = None) -> None:
        super().__init__(maxsize) #队列数量也只能maxsize
        self._last_only = last_only
        self._closed = False
        self._FTDmaxsize = maxlen
        self.debug = False
        if self._FTDmaxsize is not None:
            self._FTDdeque = collections.deque( ) #session层级总指令时间
            if "ReqQry" in self._FTDmaxsize: self._ReqQrydeque = collections.deque( ) #session层级总请求时间
            if "OrderInsert" in self._FTDmaxsize: self._OrderInsertdeque = collections.deque( ) #session层级总报单时间
            if "OrderAction" in self._FTDmaxsize: self._OrderActiondeque = collections.deque( ) #session层级总撤单时间
            if "order_exe" in self._FTDmaxsize: self._order_exedeque = collections.deque( ) #session层级总报撤单时间
            self._OrderInsertProduct = {} #记录每个品种的报单时间
            self._OrderActionProduct = {} #记录每个品种的撤单时间
            self._ex_order_exe = {} #session层级交易所总报撤单时间

    def close(self) -> None:
        """
        关闭channel
        关闭后send将不起作用,recv在收完剩余数据后会立即返回None
        """
        if not self._closed:
            self._closed = True
            self.put(None)
            

    def send(self, item: Any) -> None:
        """
        异步发送数据到channel中
        Args:
            item (any): 待发送的对象
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    self.get_nowait()
            self.put(item)

    def send_nowait(self, item: Any) -> None:
        """
        尝试立即发送数据到channel中
        Args:
            item (any): 待发送的对象
        Raises:
            QueueFull: 如果channel已满则会抛出 QueueFull
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    self.get_nowait()
            self.put_nowait( item)

    def recv(self) -> Any:
        """
        异步接收channel中的数据，如果channel中没有数据则一直等待
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        """
        if self._closed and self.empty():
            return None
        item = self.get()
        return item

    def recv_nowait(self) -> Any:
        """
        尝试立即接收channel中的数据
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        Raises:
            QueueFull: 如果channel中没有数据则会抛出 QueueEmpty
        """
        if self._closed and self.empty():
            return None
        item = self.get_nowait()
        return item

    def recv_latest(self, latest: Any) -> Any:
        """
        尝试立即接收channel中的最后一个数据
        Args:
            latest (any): 如果当前channel中没有数据或已关闭则返回该对象
        Returns:
            any: channel中的最后一个数据
        """
        while (self._closed and self.qsize() > 1) or (not self._closed and not self.empty()):
            latest = self.get_nowait()
        return latest
    
    def clear(self):
        """
        清空channel中的数据
        """
        while not self.empty():
            self.get_nowait()
        self._FTDdeque.clear()
        #请求时间重置 设置初始时间
        self._FTDdeque.appendleft(0)  
        if "ReqQry" in self._FTDmaxsize: self._ReqQrydeque.appendleft(0)
        if "OrderInsert" in self._FTDmaxsize: self._OrderInsertdeque.appendleft(0)
        if "OrderAction" in self._FTDmaxsize: self._OrderActiondeque.appendleft(0)
        if "order_exe" in self._FTDmaxsize: self._order_exedeque.appendleft(0)
    
    def add_reqtime(self,kw:dict):
        t = tm.time()  
        reqfunc = kw['reqfunc']
        product = kw.get('product', "")
        exchange = kw.get('exchange', "")
        #session层级总指令时间
        self._FTDdeque.append(t)
        self.remove_extra(self._FTDdeque,self._FTDmaxsize["Session"])
        if reqfunc == "ReqQry" and "ReqQry" in self._FTDmaxsize: 
            #session层级总请求时间
            self._ReqQrydeque.append(t)
            self.remove_extra(self._ReqQrydeque,self._FTDmaxsize["ReqQry"])

        elif reqfunc == "OrderInsert" and "OrderInsert" in self._FTDmaxsize: 
            #session层级总报单时间
            self._OrderInsertdeque.append(t)
            self.remove_extra(self._OrderInsertdeque,self._FTDmaxsize["OrderInsert"])
            #session层级总报撤单时间
            if "order_exe" in self._FTDmaxsize: 
                self._order_exedeque.append(t)
                self.remove_extra(self._order_exedeque,self._FTDmaxsize["order_exe"])
            
            if product :
                if  product not in self._OrderInsertProduct: 
                    self._OrderInsertProduct[product] = collections.deque(maxlen=self._FTDmaxsize["OrderInsert"])
                #记录每个品种的报单时间
                self._OrderInsertProduct[product].append(t)
                self.remove_extra(self._OrderInsertProduct[product],self._FTDmaxsize["OrderInsert"])
            #session层级交易所总报撤单时间
            if exchange:
                if exchange not in self._ex_order_exe:
                    self._ex_order_exe[exchange] = collections.deque(maxlen=self._FTDmaxsize["ex_order_exe"])
                self._ex_order_exe[exchange].append(t)
                self.remove_extra(self._ex_order_exe[exchange],self._FTDmaxsize["ex_order_exe"])
            
        elif reqfunc == "OrderAction" and "OrderAction" in self._FTDmaxsize: 
            #session层级总撤单时间
            self._OrderActiondeque.append(t)
            self.remove_extra(self._OrderActiondeque,self._FTDmaxsize["OrderAction"])
            #session层级总报撤单时间
            if "order_exe" in self._FTDmaxsize: 
                self._order_exedeque.append(t)
                self.remove_extra(self._order_exedeque,self._FTDmaxsize["order_exe"])
            
            if product :
                if  product not in self._OrderActionProduct: 
                    self._OrderActionProduct[product] = collections.deque(maxlen=self._FTDmaxsize["OrderAction"])
                #记录每个品种的撤单时间
                self._OrderActionProduct[product].append(t)
                self.remove_extra(self._OrderActionProduct[product],self._FTDmaxsize["OrderAction"])
            #session层级交易所总报撤单时间
            if exchange:
                if exchange not in self._ex_order_exe:
                    self._ex_order_exe[exchange] = collections.deque(maxlen=self._FTDmaxsize["ex_order_exe"])
                self._ex_order_exe[exchange].append(t)
                self.remove_extra(self._ex_order_exe[exchange],self._FTDmaxsize["ex_order_exe"])

    def enable_reqtime(self,kw:dict):
        t = tm.time()     
        reqfunc = kw['reqfunc']
        product = kw.get('product', "")
        exchange = kw.get('exchange', "")
        #session层级总指令时间满足
        ftd = not self._FTDdeque or t - self._FTDdeque[0] > 1
        req = False
        if reqfunc == "ReqQry" and "ReqQry" in self._FTDmaxsize:
            #session层级请求时间满足
            req = not self._ReqQrydeque or t - self._ReqQrydeque[0] > 1
            
        elif reqfunc == "OrderInsert" and "OrderInsert" in self._FTDmaxsize:
            #session层级报单时间满足
            req = not self._OrderInsertdeque or t - self._OrderInsertdeque[0] > 1
            #session层级报撤单时间满足
            if "order_exe" in self._FTDmaxsize: 
                req = req and (not self._order_exedeque or t - self._order_exedeque[0] > 1)
            #session层级交易所报撤单时间满足
            if exchange in self._ex_order_exe: 
                req = req and (not self._ex_order_exe[exchange] or t - self._ex_order_exe[exchange][0] > 1)
            #session层级分品种报单时间满足
            if product in self._OrderInsertProduct: 
                req = req and (not self._OrderInsertProduct[product] or t - self._OrderInsertProduct[product][0] > 1)
        elif reqfunc == "OrderAction" and "OrderAction" in self._FTDmaxsize:
            #session层级撤单时间满足
            req = not self._OrderActiondeque or t - self._OrderActiondeque[0] > 1
            #session层级报撤单时间满足
            if "order_exe" in self._FTDmaxsize: 
                req = req and (not self._order_exedeque or t - self._order_exedeque[0] > 1)
            #session层级交易所报撤单时间满足
            if exchange in self._ex_order_exe: 
                req = req and (not self._ex_order_exe[exchange] or t - self._ex_order_exe[exchange][0] > 1)
            #session层级分品种撤单时间满足
            if product in self._OrderActionProduct: 
                req = req and (not self._OrderActionProduct[product] or t - self._OrderActionProduct[product][0] > 1)
        return ftd and req

    def remove_extra(self,dq:collections.deque,keep_count):
        remove_count = max(0, len(dq) - keep_count)
        for _ in range(remove_count):
            dq.popleft()

    def __iter__(self):
        return self

    def __next__(self):
        value = self.get()
        if self._closed and self.empty():
            raise StopIteration
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class ProcessChan(multiprocessing.queues.Queue):

    def __init__(self, last_only: bool = False) -> None:
        super().__init__()
        self._last_only = last_only
        self._closed = False

    def close(self) -> None:
        """
        关闭channel
        关闭后send将不起作用,recv在收完剩余数据后会立即返回None
        """
        if not self._closed:
            self._closed = True
            self.put( None)

    def send(self, item: Any) -> None:
        """
        异步发送数据到channel中
        Args:
            item (any): 待发送的对象
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    self.get_nowait()
            self.put(item)

    def send_nowait(self, item: Any) -> None:
        """
        尝试立即发送数据到channel中
        Args:
            item (any): 待发送的对象
        Raises:
            QueueFull: 如果channel已满则会抛出 QueueFull
        """
        if not self._closed:
            if self._last_only:
                while not self.empty():
                    self.get_nowait()
            self.put_nowait(item)

    def recv(self) -> Any:
        """
        异步接收channel中的数据，如果channel中没有数据则一直等待
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        """
        if self._closed and self.empty():
            return None
        item = self.get()
        return item

    def recv_nowait(self) -> Any:
        """
        尝试立即接收channel中的数据
        Returns:
            any: 收到的数据，如果channel已被关闭则会立即收到None
        Raises:
            QueueFull: 如果channel中没有数据则会抛出 QueueEmpty
        """
        if self._closed and self.empty():
            return None
        item = self.get_nowait()
        return item

    def recv_latest(self, latest: Any) -> Any:
        """
        尝试立即接收channel中的最后一个数据
        Args:
            latest (any): 如果当前channel中没有数据或已关闭则返回该对象
        Returns:
            any: channel中的最后一个数据
        """
        while (self._closed and self.qsize() > 1) or (not self._closed and not self.empty()):
            latest = self.get_nowait()
        return latest
    
    def __iter__(self):
        return self

    def __next__(self):
        value = self.get()
        if self._closed and self.empty():
            raise StopIteration
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

class WorkThread(threading.Thread):
    # 1. 类级公共变量：所有线程实例共享
    #_global_exception = False  # 异常标志（任一线程抛异常则设为True）
    #_global_exception_info = None  # 存储异常详情
    #_global_lock = threading.Lock()  # 保证公共变量的线程安全
    # 类级计数器：用于区分同类型线程（如多个交易线程）
    _thread_counter = 0
    _counter_lock = threading.Lock()  # 确保计数器线程安全

    def __init__(self,target,args,kwargs,exception_queue:ThreadChan=None,_api=False,name_prefix=None,daemon=None):
        # 1. 生成自动名称
        self.auto_name = self._generate_name(target, name_prefix)
        # 2. 调用父类构造函数，传递自动生成的名称
        super().__init__(name=self.auto_name,daemon=daemon)
        self.exception_queue = exception_queue  # 用于传递异常的队列
        self.target = target
        self.args = args
        self.kwargs = kwargs   
        self.api = None
        self._api = _api
        self._stopped = False  # 终止标志
        self._closed = False   # ThreadChan关闭标志

    def _generate_name(self, target, name_prefix):
        """生成自动线程名的核心逻辑"""
        # 规则1：优先使用用户指定的前缀（若提供）
        if name_prefix:
            base_name = name_prefix
        # 规则2：否则使用目标函数的名称（如target为TraderApi则用"TraderApi"）
        else:
            # 获取目标函数的名称（支持类实例、函数、方法）
            if hasattr(target, "__name__"):
                base_name = target.__name__
            elif hasattr(target, "__class__"):
                base_name = target.__class__.__name__
            else:
                base_name = "WorkThread"
        
        # 规则3：添加全局计数器，避免同名线程冲突
        with WorkThread._counter_lock:
            WorkThread._thread_counter += 1
            counter = WorkThread._thread_counter
        
        # 最终名称格式：前缀+计数器（如"TraderApi-1"、"MdApi-2"）
        return f"{base_name}-{counter}"    
    def run(self):
        try:
            # 执行目标函数（如TraderApi、_rtn_thread）
            self.api = self.target(*self.args,**self.kwargs)
            # 如果是CTP API实例，等待其内部循环结束
            if self._api is True and self.api is not None: self.api._Join()
        except:
            e = traceback.format_exc()
            print(e)
            #with WorkThread._global_lock:
            #    WorkThread._global_exception = True
            #    WorkThread._global_exception_info = e
            if self.exception_queue is not None:self.exception_queue.put(e)
        finally:
            pass 
    def stop(self):
        """优雅终止线程:1. 关闭关联的ThreadChan;2. 调用API的stop方法;3. 标记终止"""
        if self._stopped:
            return
        self._stopped = True

        # 1. 如果是CTP API线程（如tradethread、mdthread），调用其内部stop方法
        if self._api and self.api is not None and hasattr(self.api, "stop"):
            self.api.stop()

        # 2. 如果线程目标是自定义循环函数（如_rtn_thread），关闭其使用的ThreadChan
        # （需确保目标函数中通过ThreadChan.recv()循环，关闭后会返回None退出）
        if hasattr(self.target, "__name__"):
            target_name = self.target.__name__
            # 示例:如果是_rtn_thread，关闭其使用的_rtn_queue
            if target_name == "_rtn_thread" and "rtn_queue" in self.kwargs:
                self.kwargs["rtn_queue"].close()

        # 3. 等待线程退出（超时1秒，避免阻塞）
        self.join(timeout=1.0)
        if self.is_alive():
            print(f"警告:线程{self.name}无法优雅终止，可能存在资源泄露")
    def _Join(self):
        if self._api is True and self.api is not None: self.api._Join()
    @classmethod
    def stop_all_threads(cls):
        """终止全局注册表中的所有WorkThread"""
        with REGISTRY_LOCK:
            # 复制一份注册表（避免迭代时修改）
            all_threads = WORK_THREAD_REGISTRY.copy()
        
        print(f"开始终止所有线程，共{len(all_threads)}个线程")
        for thread in all_threads:
            if thread.is_alive():
                print(f"终止线程：{thread.name}")
                thread.stop()
        print("所有线程终止完成")
    @classmethod
    def check_global_exception(cls) -> bool:
        """供外部检查全局异常状态的类方法"""
        with cls._global_lock:
            return cls._global_exception

    @classmethod
    def get_exception_info(cls) -> str:
        """获取异常详情"""
        with cls._global_lock:
            return cls._global_exception_info

    @classmethod
    def reset_global_exception(cls):
        """重置全局异常标志（用于重新启动线程场景）"""
        with cls._global_lock:
            cls._global_exception = False
            cls._global_exception_info = None

class WorkProcess(multiprocessing.Process):
    def __init__(self,target,args,kwargs):
        super().__init__()
        self.target = target
        self.args = args
        self.kwargs = kwargs    
    def run(self):
        self.target(*self.args,**self.kwargs)

try:
    import pygame
except: pass

class PygameAudioPlayer:
    def __init__(self,song = {}):
        # 初始化 pygame 音频模块（仅初始化一次，避免重复创建）
        pygame.mixer.init()
        self.is_playing = False  # 播放状态标记
        self.current_sound = None  # 当前播放的音频对象
        self.song = song

    def _play_worker(self, audio_path,maxtime=0):
        """子线程播放函数：负责播放+自动清理（不提前关闭 mixer）"""
        try:
            # 关键：播放前检查 mixer 是否已初始化（避免被意外关闭）
            if not pygame.mixer.get_init():
                pygame.mixer.init()  # 若已关闭，重新初始化
            
            # 加载音频文件（支持多格式：MP3/WAV/OGG 等）
            self.current_sound = pygame.mixer.Sound(audio_path)
            self.is_playing = True
            # 非阻塞播放（播放完后自动停止）
            #loops：控制播放次数（循环次数）默认值：0 → 只播放 1 次（不循环）,loops = n → 总共播放 n + 1 次,loops = -1 → 无限循环播放，直到被手动停止（stop()）或达到 maxtime 限制
            #maxtime:控制最大播放时长,默认值：0 → 无时长限制（播放完整音频，或按 loops 循环）,maxtime = t → 播放 t 毫秒后自动停止,若音频本身长度短于 maxtime，且设置了循环（loops > 0），则会循环播放直到达到 maxtime
            #fade_ms：控制淡入效果时长,fade_ms = f → 在前 f 毫秒内，音量从 0 线性增加到最大音量
            self.current_sound.play(maxtime=maxtime)
            
            # 监听播放状态：直到播放结束或手动停止
            while self.is_playing and pygame.mixer.get_busy():
                tm.sleep(0.001)  # 降低CPU占用（每0.1秒检查一次）
            
            #print(f"音频 '{audio_path}' 播放完成")
        
        except Exception as e:
            pass
            #print(f"播放失败: {e}")
        
        finally:
            # 仅清理当前音频对象，不关闭 mixer（保留模块资源供后续播放）
            self.is_playing = False
            self.current_sound = None

    def play(self,repor):
        """外部调用接口：启动子线程播放（不阻塞主线程）"""
        if self.is_playing:
            #print("已有音频在播放，先停止当前音频")
            self.stop()
        audio_path,maxtime = self.song[repor]
        # 启动子线程，避免阻塞主线程
        play_thread = threading.Thread(
            target=self._play_worker,
            args=(audio_path,maxtime)
        )
        play_thread.daemon = True  # 主线程退出时，子线程自动退出
        play_thread.start()

    def stop(self):
        """手动停止播放"""
        if self.is_playing and self.current_sound:
            self.current_sound.stop()
            self.is_playing = False  # 触发 _play_worker 中的循环退出
        #print("音频已手动停止")

    def __del__(self):
        """对象销毁时，统一关闭 mixer（释放资源）"""
        if pygame.mixer.get_init():
            pygame.mixer.quit()
            #print("音频模块资源已统一释放")

    
