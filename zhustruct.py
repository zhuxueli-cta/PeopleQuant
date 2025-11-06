#!usr/bin/env python3
#-*- coding:utf-8 -*-
__author__ = 'zhuxueli'
from typing import Dict, Any, Optional, Iterable, List, Tuple, Iterator
import polars,os
import glob,re
from datetime import datetime,date,timedelta

nReason = {4097:'网络读失败',4098:'网络写失败',8193:'接收心跳超时',8194:'发送心跳失败',8195:'收到错误报文'}
HedgeFlag={'speculation':'1',   #投机
            'arbitrage':'2',    #套利
            'hedge':'3'         #套保
           }

class FullDictDataClass():
    """支持全部字典方法的可变数据类"""
    
    def __init__(self, data: Optional[Dict[str, Any]] = None):
        """从字典初始化属性"""
        if data: self.__dict__.update(data)
    
    # ----------------------
    # 核心：字典式访问基础方法
    # ----------------------
    def __getitem__(self, key: str) -> Any:
        """支持 obj[key] 访问"""
        if key in self.__dict__:
            return self.__dict__[key]
        raise KeyError(f"键不存在: {key}")
    
    def __setitem__(self, key: str, value: Any) -> None:
        """支持 obj[key] = value 修改"""
        self.__dict__[key] = value
    
    def __delitem__(self, key: str) -> None:
        """支持 del obj[key] 删除"""
        if key in self.__dict__:
            del self.__dict__[key]
        else:
            raise KeyError(f"键不存在: {key}")
    
    def __iter__(self) -> Iterator[str]:
        """默认迭代键（与字典行为一致）"""
        return iter(self.__dict__.keys())  # 迭代键

    # ----------------------
    # 实现字典的全部方法
    # ----------------------
    def keys(self) -> Iterable[str]:
        """返回所有键，同 dict.keys()"""
        return self.__dict__.keys()
    
    def values(self) -> Iterable[Any]:
        """返回所有值，同 dict.values()"""
        return self.__dict__.values()
    
    def items(self) -> Iterable[Tuple[str, Any]]:
        """返回所有键值对，同 dict.items()"""
        return self.__dict__.items()
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取键值，不存在返回默认值，同 dict.get()"""
        return self.__dict__.get(key, default)
    
    def update(self, other: Dict[str, Any]) -> 'FullDictDataClass':
        """批量更新键值对，同 dict.update()"""
        self.__dict__.update(other)
        return self
    
    def pop(self, key: str, default: Any = None) -> Any:
        """删除并返回键值，同 dict.pop()"""
        return self.__dict__.pop(key, default)
    
    def popitem(self) -> Tuple[str, Any]:
        """删除并返回最后一个键值对，同 dict.popitem()"""
        return self.__dict__.popitem()
    
    def clear(self) -> None:
        """清空所有键值对，同 dict.clear()"""
        self.__dict__.clear()
    
    def copy(self) -> 'FullDictDataClass':
        """复制实例，同 dict.copy()"""
        return self.__class__(self.to_dict())
    
    def setdefault(self, key: str, default: Any = None) -> Any:
        """若键不存在则设置默认值，同 dict.setdefault()"""
        return self.__dict__.setdefault(key, default)
    
    # ----------------------
    # 辅助方法
    # ----------------------
    def to_dict(self) -> Dict[str, Any]:
        """转换为纯字典"""
        return self.__dict__.copy()
    
    def __len__(self) -> int:
        """支持 len(obj) 获取键值对数量"""
        return len(self.__dict__)
    
    def __contains__(self, key: str) -> bool:
        """支持 'key' in obj 判断键是否存在"""
        return key in self.__dict__
    
    def __repr__(self) -> str:
        """实例打印格式"""
        return f"{self.__class__.__name__}({self.__dict__})"
    
    def __str__(self) -> str:
        """实例打印格式"""
        return f"{self.__dict__}"
    
# ----------------------
# 具体数据类（继承增强版基类）
# ----------------------
class Account(FullDictDataClass):
    """账户数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #经纪公司代码
        self.BrokerID = ""
        #投资者帐号
        self.AccountID = ""
        #上次质押金额
        self.PreMortgage = 0
        #上次信用额度
        self.PreCredit = 0
        #上次存款额
        self.PreDeposit = 0
        #上次结算准备金
        self.PreBalance = 0
        #上次占用的保证金
        self.PreMargin = 0
        #利息基数
        self.InterestBase = 0
        #利息收入
        self.Interest = 0
        #入金金额
        self.Deposit = 0
        #出金金额
        self.Withdraw = 0
        #冻结的保证金
        self.FrozenMargin = 0
        #冻结的资金
        self.FrozenCash = 0
        #冻结的手续费
        self.FrozenCommission = 0
        #当前保证金总额
        self.CurrMargin = 0
        #资金差额
        self.CashIn = 0
        #手续费
        self.Commission = 0
        #平仓盈亏
        self.CloseProfit = 0
        #持仓盈亏
        self.PositionProfit = 0
        #期货结算准备金
        self.Balance = 0
        #可用资金
        self.Available = 0
        #可取资金
        self.WithdrawQuota = 0
        #基本准备金
        self.Reserve = 0
        #交易日
        self.TradingDay = 0
        #结算编号
        self.SettlementID = ""
        #信用额度
        self.Credit = 0
        #质押金额
        self.Mortgage = 0
        #交易所保证金
        self.ExchangeMargin = 0
        #投资者交割保证金
        self.DeliveryMargin = 0
        #交易所交割保证金
        self.ExchangeDeliveryMargin = 0
        #保底期货结算准备金
        self.ReserveBalance = 0
        #币种代码
        self.CurrencyID = "CNY"
        #上次货币质入金额
        self.PreFundMortgageIn = 0
        #上次货币质出金额
        self.PreFundMortgageOut = 0
        #货币质入金额
        self.FundMortgageIn = 0
        #货币质出金额
        self.FundMortgageOut = 0
        #货币质押余额
        self.FundMortgageAvailable = 0
        #可质押货币金额--已废弃
        self.MortgageableFund = 0
        #特殊产品占用保证金--已废弃
        self.SpecProductMargin = 0
        #特殊产品冻结保证金--已废弃 
        self.SpecProductFrozenMargin = 0
        #特殊产品手续费--已废弃
        self.SpecProductCommission = 0
        #特殊产品冻结手续费--已废弃
        self.SpecProductFrozenCommission = 0
        #特殊产品持仓盈亏--已废弃
        self.SpecProductPositionProfit = 0
        #特殊产品平仓盈亏--已废弃
        self.SpecProductCloseProfit = 0
        #根据持仓盈亏算法计算的特殊产品持仓盈亏--已废弃
        self.SpecProductPositionProfitByAlg = 0
        #特殊产品交易所保证金--已废弃
        self.SpecProductExchangeMargin = 0
        #业务类型
        self.BizType = ""
        #延时换汇冻结金额
        self.FrozenSwap = 0
        #剩余换汇额度
        self.RemainSwap = 0
        #期权市值
        self.OptionValue = 0
        #本地时间戳
        self.local_timestamp = 0
        #风险度
        self.risk_ratio = 0
        #浮动盈亏
        self.float_profit = 0

class Position(FullDictDataClass):
    """持仓数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #多头持仓
        self.pos_long = 0
        #多头今仓
        self.pos_long_today = 0
        #多头昨仓
        self.pos_long_his = 0
        #多头开仓均价
        self.open_price_long = float("nan")
        #多头持仓均价
        self.position_price_long = float("nan")
        #空头持仓
        self.pos_short = 0
        #空头今仓
        self.pos_short_today = 0
        #空头昨仓
        self.pos_short_his = 0
        #空头开仓均价
        self.open_price_short = float("nan")
        #空头持仓均价
        self.position_price_short = float("nan")
        #多头持仓盈亏
        self.position_profit_long = float("nan")
        #多头浮动盈亏
        self.float_profit_long = float("nan")
        #空头持仓盈亏
        self.position_profit_short = float("nan")
        #空头浮动盈亏
        self.float_profit_short = float("nan")
        #多空总保证金
        self.margin = float("nan")
        #交易所多空总保证金
        self.exch_margin = float("nan")
        #多头保证金
        self.margin_long = float("nan")
        #空头保证金
        self.margin_short = float("nan")
        #多头保证金率(按金额)
        self.margin_rate_long = float("nan")
        #空头保证金率(按金额)
        self.margin_rate_short = float("nan")
        #多头保证金率(按手数)
        self.margin_volume_long = float("nan")
        #空头保证金率(按手数)
        self.margin_volume_short = float("nan")
        #合约代码
        self.instrument_id = ""
        #交易所代码
        self.exchange_id = ""
        #期货"FUTURE",看涨期权"CALL",看跌期权"PUT"
        self.ins_class = ""
        #期权行权价
        self.strike_price  = float("nan")
        #空头今仓冻结
        self.short_frozen_today = 0
        #多头今仓冻结
        self.long_frozen_today = 0
        #空头昨仓冻结
        self.short_frozen_his = 0
        #多头昨仓冻结
        self.long_frozen_his = 0
        #开仓手续费率(按金额)
        self.OpenRatioByMoney = 0
        #开仓手续费(按手数)
        self.OpenRatioByVolume = 0
        #平仓手续费率(按金额)
        self.CloseRatioByMoney = 0
        #平仓手续费(按手数)
        self.CloseRatioByVolume = 0
        #平今仓手续费率(按金额)
        self.CloseTodayRatioByMoney = 0
         #平今仓手续费(按手数)
        self.CloseTodayRatioByVolume = 0
        #本地时间戳
        self.local_timestamp = 0

class Trade(FullDictDataClass):
    """成交单数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #经纪公司代码
        self.BrokerID = ""
        #投资者代码
        self.InvestorID = ""
        #保留的无效字段
        self.reserve1 = ""
        #报单引用
        self.OrderRef = ""
        #用户代码
        self.UserID = ""
        #交易所代码
        self.ExchangeID = ""
        #成交编号
        self.TradeID = ""
        #买卖方向
        self.Direction = ""
        #报单编号
        self.OrderSysID = ""
        #会员代码
        self.ParticipantID = ""
        #客户代码
        self.ClientID = ""
        #交易角色
        self.TradingRole = ""
        #保留的无效字段
        self.reserve2 = ""
        #开平标志
        self.OffsetFlag = ""
        #投机套保标志
        self.HedgeFlag = ""
        #价格
        self.Price = float("nan")
        #数量
        self.Volume = float("nan")
        #成交时期
        self.TradeDate = ""
        #成交时间
        self.TradeTime = ""
        #成交类型
        self.TradeType = ""
        #成交价来源
        self.PriceSource = ""
        #交易所交易员代码
        self.TraderID = ""
        #本地报单编号
        self.OrderLocalID = ""
        #结算会员编号
        self.ClearingPartID = ""
        #业务单元
        self.BusinessUnit = ""
        #序号
        self.SequenceNo = ""
        #交易日
        self.TradingDay = ""
        #结算编号
        self.SettlementID = ""
        #经纪公司报单编号
        self.BrokerOrderSeq = ""
        #成交来源
        self.TradeSource = ""
        #投资单元代码
        self.InvestUnitID = ""
        #合约代码
        self.InstrumentID = ""
        #合约在交易所的代码
        self.ExchangeInstID = ""
        #本地时间戳
        self.local_timestamp = 0

class Order(FullDictDataClass):
    """委托单数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #经纪公司代码
        self.BrokerID = ""
        #投资者代码
        self.InvestorID = ""
        #保留的无效字段
        self.reserve1 = ""
        #报单引用
        self.OrderRef = ""
        #用户代码
        self.UserID = ""
        #报单价格条件
        self.OrderPriceType = ""
        #买卖方向
        self.Direction = ""
        #组合开平标志
        self.CombOffsetFlag = ""
        #组合投机套保标志
        self.CombHedgeFlag = ""
        #价格
        self.LimitPrice = float("nan")
        #数量
        self.VolumeTotalOriginal = float("nan")
        #有效期类型
        self.TimeCondition = ""
        #GTD日期
        self.GTDDate = ""
        #成交量类型
        self.VolumeCondition = ""
        #最小成交量
        self.MinVolume = float("nan")
        #触发条件
        self.ContingentCondition = ""
        #止损价
        self.StopPrice = ""
        #强平原因
        self.ForceCloseReason = ""
        #自动挂起标志
        self.IsAutoSuspend = ""
        #业务单元
        self.BusinessUnit = ""
        #请求编号
        self.RequestID = ""
        #本地报单编号
        self.OrderLocalID = ""
        #交易所代码
        self.ExchangeID = ""
        #会员代码
        self.ParticipantID = ""
        #客户代码
        self.ClientID = ""
        #保留的无效字段
        self.reserve2 = ""
        #交易所交易员代码
        self.TraderID = ""
        #安装编号
        self.InstallID = ""
        #报单提交状态
        self.OrderSubmitStatus = ""
        #报单提示序号
        self.NotifySequence = ""
        #交易日
        self.TradingDay = ""
        #结算编号
        self.SettlementID = ""
        #报单编号
        self.OrderSysID = ""
        #报单来源
        self.OrderSource = ""
        #报单状态
        self.OrderStatus = ""
        #报单类型
        self.OrderType = ""
        #今成交数量
        self.VolumeTraded = float("nan")
        #剩余数量
        self.VolumeTotal = float("nan")
        #报单日期
        self.InsertDate = ""
        #委托时间
        self.InsertTime = ""
        #激活时间
        self.ActiveTime = ""
        #挂起时间
        self.SuspendTime = ""
        #最后修改时间
        self.UpdateTime = ""
        #撤销时间
        self.CancelTime = ""
        #最后修改交易所交易员代码
        self.ActiveTraderID = ""
        #结算会员编号
        self.ClearingPartID = ""
        #序号
        self.SequenceNo = ""
        #前置编号
        self.FrontID = ""
        #会话编号
        self.SessionID = ""
        #用户端产品信息
        self.UserProductInfo = ""
        #状态信息
        self.StatusMsg = ""
        #用户强平标志
        self.UserForceClose = ""
        #操作用户代码
        self.ActiveUserID = ""
        #经纪公司报单编号
        self.BrokerOrderSeq = ""
        #相关报单
        self.RelativeOrderSysID = ""
        #郑商所成交数量
        self.ZCETotalTradedVolume = ""
        #互换单标志
        self.IsSwapOrder = ""
        #营业部编号
        self.BranchID = ""
        #投资单元代码
        self.InvestUnitID = ""
        #资金账号
        self.AccountID = ""
        #币种代码
        self.CurrencyID = ""
        #保留的无效字段
        self.reserve3 = ""
        #Mac地址
        self.MacAddress = ""
        #合约代码
        self.InstrumentID = ""
        #合约在交易所的代码
        self.ExchangeInstID = ""
        #IP地址
        self.IPAddress = ""
        #报单回显字段
        self.OrderMemo = ""
        #session上请求计数 api自动维护
        self.SessionReqSeq = ""
        #本地时间戳
        self.local_timestamp = 0

class Quote(FullDictDataClass):
    """行情快照数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #交易日
        self.TradingDay = ""
        #合约代码
        self.InstrumentID = ""
        #交易所代码
        self.ExchangeID = ""
        #合约在交易所的代码
        self.ExchangeInstID = ""
        #最新价
        self.LastPrice = float("nan")
        #上次结算价
        self.PreSettlementPrice = float("nan")
        #昨收盘
        self.PreClosePrice = float("nan")
        #昨持仓量
        self.PreOpenInterest = float("nan")
        #今开盘
        self.OpenPrice = float("nan")
        #最高价
        self.HighestPrice = float("nan")
        #最低价
        self.LowestPrice = float("nan")
        #数量
        self.Volume = float("nan")
        #成交金额
        self.Turnover = float("nan")
        #持仓量
        self.OpenInterest = float("nan")
        #今收盘
        self.ClosePrice = float("nan")
        #本次结算价
        self.SettlementPrice = float("nan")
        #涨停板价
        self.UpperLimitPrice = float("nan")
        #跌停板价
        self.LowerLimitPrice = float("nan")
        #昨虚实度
        self.PreDelta = float("nan")
        #今虚实度
        self.CurrDelta = float("nan")
        #最后修改时间
        self.UpdateTime = ""
        #最后修改毫秒
        self.UpdateMillisec = float("nan")
        #申买价一
        self.BidPrice1 = float("nan")
        #申买量一
        self.BidVolume1 = float("nan")
        #申卖价一
        self.AskPrice1 = float("nan")
        #申卖量一
        self.AskVolume1 = float("nan")
        #申买价二
        self.BidPrice2 = float("nan")
        #申买量二
        self.BidVolume2 = float("nan")
        #申卖价二
        self.AskPrice2 = float("nan")
        #申卖量二
        self.AskVolume2 = float("nan")
        #申买价三
        self.BidPrice3 = float("nan")
        #申买量三
        self.BidVolume3 = float("nan")
        #申卖价三
        self.AskPrice3 = float("nan")
        #申卖量三
        self.AskVolume3 = float("nan")
        #申买价四
        self.BidPrice4 = float("nan")
        #申买量四
        self.BidVolume4 = float("nan")
        #申卖价四
        self.AskPrice4 = float("nan")
        #申卖量四
        self.AskVolume4 = float("nan")
        #申买价五
        self.BidPrice5 = float("nan")
        #申买量五
        self.BidVolume5 = float("nan")
        #申卖价五
        self.AskPrice5 = float("nan")
        #申卖量五
        self.AskVolume5 = float("nan")
        #当日均价
        self.AveragePrice = float("nan")
        #业务日期
        self.ActionDay = float("nan")
        #上带价
        self.BandingUpperPrice = float("nan")
        #下带价
        self.BandingLowerPrice = float("nan")
        #本地时间戳
        self.local_timestamp = 0
        #CTP行情时间戳
        self.ctp_timestamp = 0
        #CTP行情datetime
        self.ctp_datetime = 0

class InstrumentProperty(FullDictDataClass):
    """行情快照数据类"""
    def __init__( self,
        # 允许动态传入其他字段（**kwargs）
        **kwargs
    ):
        #  处理动态扩展字段（通过基类初始化）
        super().__init__(kwargs)  # 等价于：self.__dict__.update(kwargs)
        #保留的无效字段
        self.reserve1 = ""
        #交易所代码
        self.ExchangeID = ""
        #合约名称
        self.InstrumentName = ""
        #保留的无效字段
        self.reserve2 = ""
        #保留的无效字段
        self.reserve3 = ""
        #产品类型
        self.ProductClass = ""
        #交割年份
        self.DeliveryYear = ""
        #交割月
        self.DeliveryMonth = ""
        #市价单最大下单量
        self.MaxMarketOrderVolume = float("nan")
        #市价单最小下单量
        self.MinMarketOrderVolume = float("nan")
        #限价单最大下单量
        self.MaxLimitOrderVolume = float("nan")
        #限价单最小下单量
        self.MinLimitOrderVolume = float("nan")
        #合约数量乘数
        self.VolumeMultiple = float("nan")
        #最小变动价位
        self.PriceTick = float("nan")
        #创建日
        self.CreateDate = ""
        #上市日
        self.OpenDate = ""
        #到期日
        self.ExpireDate = ""
        #开始交割日
        self.StartDelivDate = ""
        #结束交割日
        self.EndDelivDate = ""
        #合约生命周期状态
        self.InstLifePhase = ""
        #当前是否交易
        self.IsTrading = ""
        #持仓类型
        self.PositionType = ""
        #持仓日期类型
        self.PositionDateType = float("nan")
        #多头保证金率
        self.LongMarginRatio = 0
        #空头保证金率
        self.ShortMarginRatio = 0
        #是否使用大额单边保证金算法
        self.MaxMarginSideAlgorithm = ""
        #保留的无效字段
        self.reserve4 = ""
        #执行价
        self.StrikePrice = float("nan")
        #期权类型
        self.OptionsType = ""
        #合约基础商品乘数
        self.UnderlyingMultiple = float("nan")
        #组合类型
        self.CombinationType = ""
        #合约代码
        self.InstrumentID = ""
        #合约在交易所的代码
        self.ExchangeInstID = ""
        #产品代码
        self.ProductID = ""
        #基础商品代码
        self.UnderlyingInstrID = ""
        #到期剩余日
        self.expire_rest_days = float('nan')



# 配置参数
# 支持的时间单位（与Polars兼容）
SUPPORTED_UNITS = {
    "s": {"full_name": "seconds", "desc": "秒"},    # 周期缩写→完整单位名称映射
    "m": {"full_name": "minutes", "desc": "分钟"},
    "h": {"full_name": "hours", "desc": "小时"},
    "d": {"full_name": "days", "desc": "天"},
    "w": {"full_name": "weeks", "desc": "周"},
    "M": {"full_name": "months", "desc": "月"},     # 注意：pl.duration不支持months，需特殊处理
    "y": {"full_name": "years", "desc": "年"}       # pl.duration不支持years，需特殊处理
}
# 预定义常用周期（可扩展）
COMMON_CYCLES = ["1s", "3s", "5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "4h", "1d", "1w"]
# 区分日内/日级周期的单位阈值
INTRADAY_UNITS = {"s", "m", "h"}  # 日内周期单位
DAILY_UNITS = {"d", "w", "M", "y"}  # 日及以上周期单位

class MutableDataHolder():
    """通用可变数据容器基类，封装DataFrame引用实现自动更新"""
    def __init__(self, schema: Dict[str, polars.DataType]):
        # 初始化空DataFrame（使用指定schema确保结构一致性）
        self._data: polars.DataFrame = polars.DataFrame(
            {col: [] for col in schema.keys()},
            schema=schema
        )

    @property
    def data(self) -> polars.DataFrame:
        """获取最新数据（返回引用，不复制）"""
        return self._data

    @data.setter
    def data(self, new_data: polars.DataFrame) -> None:
        """更新数据（替换引用，不修改原对象）"""
        # 校验列结构一致性
        required_cols = set(self._data.columns)
        if not required_cols.issubset(set(new_data.columns)):
            missing_cols = required_cols - set(new_data.columns)
            raise ValueError(f"新数据缺少必要列：{missing_cols}")
        
        self._data = new_data

    def tail(self, n: int) -> polars.DataFrame:
        """便捷方法：获取最新n条数据"""
        return self._data.tail(n)


class MutableTickHolder(MutableDataHolder):
    """Tick数据可变容器，定义Tick专属schema"""
    def __init__(self):
        tick_schema = {
            "TradingDay": polars.String,
            "ActionDay": polars.String,
            "InstrumentID": polars.String,
            "ExchangeID": polars.String,
            "LastPrice": polars.Float64,
            "Volume": polars.Int64,
            "Turnover": polars.Float64,
            "OpenInterest": polars.Float64,
            "BidPrice1": polars.Float64,
            "BidVolume1": polars.Int64,
            "AskPrice1": polars.Float64,
            "AskVolume1": polars.Int64,
            "UpdateTime": polars.String,
            "UpdateMillisec": polars.Int64,
            "ctp_datetime": polars.Datetime,
            "trading_day": polars.Date,
            "is_valid": polars.Boolean
        }
        super().__init__(tick_schema)


class MutableKlineHolder(MutableDataHolder):
    """K线数据可变容器，定义K线专属schema"""
    def __init__(self):
        kline_schema = {
            "InstrumentID": polars.String,
            "open": polars.Float64,
            "high": polars.Float64,
            "low": polars.Float64,
            "close": polars.Float64,
            "Volume": polars.Int64,
            "Turnover": polars.Float64,
            "OpenInterest": polars.Float64,
            "period_start": polars.Datetime,
            "period_end": polars.Datetime,
            "period": polars.String
        }
        super().__init__(kline_schema)

class CTPDataProcessor():
    def __init__(self, storage_format: str = "parquet",_logfile=""):
        """初始化处理器，支持选择存储格式（parquet或csv）"""
        if not _logfile:
            try:
                self._flowfile = os.path.dirname(os.path.abspath(__file__)) #当前程序目录(该py文件__file__所在目录)
            except: self._flowfile = os.getcwd()   #程序工作目录下创建目录 
            self._logfile = fr"{self._flowfile}\logs"
        else: self._logfile = _logfile
        self.tick_dir = fr"{self._logfile}\tick_data"
        self.kline_dir = fr"{self._logfile}\kline_data"
        # 创建数据目录
        os.makedirs(self.tick_dir, exist_ok=True)
        os.makedirs(self.kline_dir, exist_ok=True)
        
        # 验证存储格式
        self.storage_format = storage_format.lower()
        if self.storage_format not in ["parquet", "csv"]:
            raise ValueError(f"不支持的存储格式：{storage_format}，仅支持 'parquet' 和 'csv'")
        
        # 数据缓存：使用可变容器
        self.tick_cache: Dict[str, MutableTickHolder] = {}  # {合约ID: 可变Tick容器}
        self.kline_cache: Dict[str, Dict[str, MutableKlineHolder]] = {}  # {合约ID: {周期: 可变K线容器}}
        self.kline_periods = {}  #K线周期列表
        self.max_tick_count = {}  #最大缓存Tick数量
        self.max_kline_count = {}  #最大缓存kline数量

        # 加载本地数据
        self._load_local_data()

    # 工具方法：周期单位转换 
    def _get_duration_param(self, unit: str, value: int) -> Dict[str, int]:
        """
        将周期缩写（如'm'）转换为pl.duration()的合法参数
        返回格式：{完整单位名称: 数值}，如{"minutes": 5}
        """
        if unit not in SUPPORTED_UNITS:
            raise ValueError(f"不支持的周期单位：{unit}")
        
        full_unit = SUPPORTED_UNITS[unit]["full_name"]
        # 特殊处理：pl.duration不支持months/years，用days近似（或根据业务调整）
        if full_unit in ["months", "years"]:
            raise ValueError(f"pl.duration不支持{SUPPORTED_UNITS[unit]['desc']}单位，请使用dt.offset_by替代")
        
        return {full_unit: value}
    # 文件路径管理
    def _get_tick_file_path(self, instrument_id: str) -> str:
        """获取Tick文件路径（根据存储格式自动选择扩展名）"""
        return os.path.join(self.tick_dir, f"{instrument_id}_tick.{self.storage_format}")

    def _get_kline_file_path(self, instrument_id: str, period: str) -> str:
        """获取K线文件路径（根据存储格式自动选择扩展名）"""
        period_dir = os.path.join(self.kline_dir, period)
        os.makedirs(period_dir, exist_ok=True)
        return os.path.join(period_dir, f"{instrument_id}_kline.{self.storage_format}")

    # 本地数据加载
    def _load_local_data(self) -> None:
        """加载本地所有合约的历史数据"""
        # 查找对应格式的Tick文件
        tick_files = glob.glob(os.path.join(self.tick_dir, f"*.{self.storage_format}"))
        
        for file_path in tick_files:
            try:
                # 解析合约ID（文件名格式：{合约ID}_tick.xxx）
                file_name = os.path.basename(file_path)
                instrument_id = file_name.replace(f"_tick.{self.storage_format}", "")
                
                # 读取并解析Tick数据
                tick_df = self._read_tick_file(file_path)
                #print(f"加载 {instrument_id} 历史Tick数据：共 {len(tick_df)} 条")

                # 初始化Tick容器
                tick_holder = MutableTickHolder()
                tick_holder.data = tick_df
                self.tick_cache[instrument_id] = tick_holder
                
            except Exception as e:
                print(f"加载 {file_path} 失败：{str(e)}")

    def _read_tick_file(self, file_path: str) -> polars.DataFrame:
        """读取本地Tick文件（自动适配parquet和csv格式）"""
        try:
            if file_path.endswith(".parquet"):
                # 读取Parquet文件（保留原始数据类型）
                tick_df = polars.read_parquet(file_path)
            else:
                # 读取CSV文件（需手动解析时间字段）
                tick_df = polars.read_csv(
                    file_path,
                    parse_dates=["ctp_datetime", "trading_day"]
                )
            
            # 确保时间类型正确
            if "ctp_datetime" in tick_df.columns and not isinstance(tick_df["ctp_datetime"].dtype, polars.Datetime):
                tick_df = tick_df.with_columns(
                    polars.col("ctp_datetime").str.strptime(polars.Datetime, "%Y-%m-%d %H:%M:%S%.f").alias("ctp_datetime")
                )
            if "trading_day" in tick_df.columns and not isinstance(tick_df["trading_day"].dtype, polars.Date):
                tick_df = tick_df.with_columns(
                    polars.col("trading_day").str.strptime(polars.Date, "%Y-%m-%d").alias("trading_day")
                )
                
            return tick_df.sort("ctp_datetime")
            
        except Exception as e:
            raise ValueError(f"解析Tick文件 {file_path} 失败：{str(e)}")

    # Tick数据处理
    def process_snapshot(self, snapshot, save_instrument_id: str = "") -> None:
        """处理单条CTP快照数据"""
        
        instrument_id = snapshot['InstrumentID']
        if not save_instrument_id: save_instrument_id = instrument_id
        tick_record = self._snapshot_to_tick(snapshot)
        self._update_tick_cache(instrument_id, tick_record)
        self._append_tick_to_file(save_instrument_id, tick_record)
        # 更新所有周期K线
        for period in self.kline_periods[instrument_id]:
            self._update_kline(instrument_id, period,self.max_kline_count[instrument_id][period])
        #print({"process_snapshot":snapshot})

    def process_snapshots_batch(self, snapshots: List, save_instrument_id: str = "") -> None:
        """批量处理CTP快照（提升高频场景性能）"""
        if not snapshots:
            return
            
        # 按合约分组处理
        from collections import defaultdict
        grouped_snapshots = defaultdict(list)
        for snap in snapshots:
            grouped_snapshots[snap['InstrumentID']].append(snap)
        
        for instrument_id, snaps in grouped_snapshots.items():
            # 批量转换为Tick
            tick_records = [self._snapshot_to_tick(snap) for snap in snaps]
            new_ticks = polars.DataFrame(tick_records)
            
            # 批量更新缓存和文件
            if not save_instrument_id: save_instrument_id = instrument_id
            self._update_tick_cache_batch(instrument_id, new_ticks)
            self._append_ticks_batch(save_instrument_id, new_ticks)
            
            # 更新K线
            for period in self.kline_periods[instrument_id]:
                self._update_kline(instrument_id, period,self.max_kline_count[instrument_id][period])

    def _snapshot_to_tick(self, snapshot) -> Dict:
        """将CTP快照转换为标准化Tick（含双重时间标识）"""
        # 1. 自然时间（物理时间流）
        action_day = snapshot["ActionDay"]  # 格式："20231010"
        try:
            natural_time_str = f"{action_day} {snapshot['UpdateTime']}.{snapshot['UpdateMillisec']:03d}"
            ctp_datetime = datetime.strptime(natural_time_str, "%Y%m%d %H:%M:%S.%f")
        except ValueError as e:
            ctp_datetime = datetime.strptime(f"{action_day} {snapshot['UpdateTime']}", "%Y%m%d %H:%M:%S")
        
        # 2. 交易日标识（业务日期）
        trading_day = snapshot['TradingDay']  # 格式："20231011"
        trading_day_date = datetime.strptime(trading_day, "%Y%m%d").date()
        snapshot.update({"ctp_datetime": ctp_datetime,"trading_day": trading_day_date, "is_valid": snapshot['LastPrice'] > 0})
        return snapshot

    def _update_tick_cache(self, instrument_id: str, tick_record: Dict) -> None:
        """更新单个Tick到缓存"""
        new_tick = polars.DataFrame([tick_record])
        self._update_tick_cache_batch(instrument_id, new_tick)
        

    def _update_tick_cache_batch(self, instrument_id: str, new_ticks: polars.DataFrame) -> None:
        """批量更新Tick缓存"""
        # 初始化容器（首次更新时）
        if instrument_id not in self.tick_cache:
            self.tick_cache[instrument_id] = MutableTickHolder()
        
        tick_holder = self.tick_cache[instrument_id]
        current_ticks = tick_holder.data
        # 合并数据并去重
        if len(current_ticks) > 0:
            combined = polars.concat([current_ticks, new_ticks])
            combined = combined.sort("ctp_datetime").unique(subset=["ctp_datetime"], keep="last")
            # 限制缓存大小
            if len(combined) > self.max_tick_count[instrument_id]:
                combined = combined.tail(self.max_tick_count[instrument_id])
            tick_holder.data = combined
        else:
            # 首次添加
            tick_holder.data = new_ticks.sort("ctp_datetime")

    # Tick数据持久化（双格式支持）
    def _append_tick_to_file(self, instrument_id: str, tick_record: Dict) -> None:
        """追加单个Tick到文件"""
        self._append_ticks_batch(instrument_id, polars.DataFrame([tick_record]))

    def _append_ticks_batch(self, instrument_id: str, tick_df: polars.DataFrame) -> None:
        """批量追加Tick到文件（根据存储格式选择方式）"""
        file_path = self._get_tick_file_path(instrument_id)
        
        # 格式化时间字段（便于存储）
        formatted_df = tick_df.with_columns([
            polars.col("ctp_datetime").dt.strftime("%Y-%m-%d %H:%M:%S%.f").alias("ctp_datetime"),
            polars.col("trading_day").dt.strftime("%Y-%m-%d").alias("trading_day")
        ])
        
        if self.storage_format == "parquet":
            # Parquet不支持追加，需先读再合并（适合大数据量）
            if os.path.exists(file_path):
                existing_df = polars.read_parquet(file_path)
                combined_df = polars.concat([existing_df, formatted_df])
                # 去重并限制大小
                combined_df = combined_df.unique(subset=["ctp_datetime"], keep="last").sort("ctp_datetime")
                #if len(combined_df) > self.max_tick_count[instrument_id]:
                #    combined_df = combined_df.tail(self.max_tick_count[instrument_id])
                combined_df.write_parquet(file_path)
            else:
                formatted_df.write_parquet(file_path)
        else:
            # CSV支持追加（适合小数据量或调试）
            include_header = not os.path.exists(file_path)
            with open(file_path, "a", encoding="utf-8") as f:
                formatted_df.write_csv( f, include_header=include_header )

    # K线生成与更新
    @staticmethod
    def _validate_period(period: str) -> Tuple[bool, Optional[str]]:
        """验证周期格式合法性"""
        pattern = re.compile(r"^(\d+)([smhdwMy])$")
        match = pattern.match(period)
        if not match:
            return False, f"周期格式错误：{period}（正确格式：数字+单位，如'1s'）"
        
        unit = match.group(2)
        if unit not in SUPPORTED_UNITS:
            return False, f"不支持的单位：{unit}（支持：{list(SUPPORTED_UNITS.keys())}）"
        
        return True, None

    @staticmethod
    def _classify_period(period: str) -> Tuple[str, int, str]:
        """解析周期为（类型, 数值, 单位）"""
        pattern = re.compile(r"^(\d+)([smhdwMy])$")
        match = pattern.match(period)
        if not match:
            raise ValueError(f"无效周期格式：{period}")
        
        num = int(match.group(1))
        unit = match.group(2)
        period_type = "intraday" if unit in INTRADAY_UNITS else "daily"
        
        return period_type, num, unit

    def generate_any_period_kline( self, instrument_id: str, tick_df: polars.DataFrame, period: str, kline_count: int ) -> polars.DataFrame:
        """生成任意周期K线（日内用自然时间，日级用交易日）"""
        # 验证周期
        is_valid, msg = self._validate_period(period)
        if not is_valid:
            raise ValueError(f"生成K线失败：{msg}")
        
        # 解析周期类型
        period_type, num, unit = self._classify_period(period)

        # 选择时间基准
        if period_type == "intraday":
            time_col = "ctp_datetime"
            if not isinstance(tick_df[time_col].dtype, polars.Datetime):
                tick_df = tick_df.with_columns(polars.col(time_col).cast(polars.Datetime).alias(time_col))
        else:
            time_col = "trading_day"
            tick_df = tick_df.with_columns(polars.col(time_col).cast(polars.Date).alias(time_col))

        # 过滤必要Tick
        filtered_ticks = self._filter_needed_ticks(tick_df, time_col, period, self.max_kline_count[instrument_id][period], instrument_id)
        self.max_tick_count[instrument_id] = max(self.max_tick_count[instrument_id], len(filtered_ticks))
        # 生成K线
        if period_type == "intraday":
            kline_df = (
                filtered_ticks.group_by_dynamic(
                    time_col, by="InstrumentID", every=period, closed="left", include_boundaries=True
                )
                .agg(self._get_kline_aggs(time_col))
                .with_columns(polars.lit(period).alias("period"))
                .sort(time_col)
            )
        else:
            kline_df = (
                filtered_ticks.group_by([polars.col("InstrumentID"), polars.col(time_col).alias("kline_date")])
                .agg(self._get_kline_aggs(time_col))
                .with_columns([
                    polars.lit(period).alias("period"),
                    polars.col("kline_date").alias("kline_start_time"),
                    polars.col("kline_date").alias("kline_end_time")
                ])
                .sort("kline_date")
            )
        
        # 限制数量
        return kline_df.tail(kline_count) if len(kline_df) > kline_count else kline_df

    def _get_kline_aggs(self, time_col: str) -> List:
        """K线聚合逻辑"""
        return [
            polars.col("LastPrice").first().alias("open"),
            polars.col("LastPrice").max().alias("high"),
            polars.col("LastPrice").min().alias("low"),
            polars.col("LastPrice").last().alias("close"),
            (polars.col("Volume").last() - polars.col("Volume").first()).alias("Volume"),
            (polars.col("Turnover").last() - polars.col("Turnover").first()).alias("Turnover"),
            polars.col("OpenInterest").last().alias("OpenInterest"),
            polars.col(time_col).first().alias("period_start"),
            polars.col(time_col).last().alias("period_end")
        ]

    def _filter_needed_ticks(self, tick_df: polars.DataFrame, time_col: str, period: str, kline_count: int,instrument_id="") -> polars.DataFrame:
        """过滤生成K线所需的Tick数据"""
        if len(tick_df) == 0:
            return tick_df
        
        period_type, num, unit = self._classify_period(period)
        buffer_periods = 2
        total_offset_value = num * (kline_count + buffer_periods)

        latest_time = tick_df.select(polars.col(time_col).max()).item()
        tick_earliest_time = tick_df.select(polars.col(time_col).min()).item()

        # 计算最早保留时间（加缓冲避免边界丢失）
        if period_type == "intraday":
            # 日内周期：用pl.duration计算固定时间偏移
            duration_param = self._get_duration_param(unit, total_offset_value)
            earliest_expr = polars.lit(latest_time) - polars.duration(** duration_param)  # 传入合法参数
            earliest_time = tick_df.select(earliest_expr).item()
        else:
            days_per_unit = {"d": 1, "w": 7, "M": 30, "y": 365}.get(unit, 1)
            earliest_time = latest_time - timedelta(days=total_offset_value * days_per_unit)
        if tick_earliest_time > earliest_time: #数据量不足重新读取本地数据
            #instrument_id = tick_df.tail(1).get_column("InstrumentID").item()
            self._load_instrument_data(instrument_id)
            tick_df = self.tick_cache[instrument_id].data
            
        return tick_df.filter(polars.col(time_col) >= earliest_time)

    def _update_kline(self, instrument_id: str, period: str, kline_count: int  ) -> None:
        """更新K线缓存"""
        if instrument_id not in self.tick_cache:
            return

        # 初始化K线容器
        if instrument_id not in self.kline_cache:
            self.kline_cache[instrument_id] = {}
        if period not in self.kline_cache[instrument_id]:
            self.kline_cache[instrument_id][period] = MutableKlineHolder()

        # 获取数据
        tick_holder = self.tick_cache[instrument_id]
        tick_df = tick_holder.data
        kline_holder = self.kline_cache[instrument_id][period]
        current_kline = kline_holder.data

        # 首次生成
        if len(current_kline) == 0:
            new_kline = self.generate_any_period_kline(instrument_id, tick_df, period, kline_count)
            kline_holder.data = new_kline
            self._save_kline_to_file(instrument_id, period, new_kline)
            return

        # 增量更新
        period_type, _, _ = self._classify_period(period)
        time_col = "ctp_datetime" if period_type == "intraday" else "trading_day"
        latest_period_end = current_kline.select(polars.col("period_end").max()).item()
        new_ticks = tick_df.filter(polars.col(time_col) > latest_period_end)

        if len(new_ticks) == 0:
            return

        new_klines = self.generate_any_period_kline(instrument_id, new_ticks, period, kline_count + 5)
        if len(new_klines) == 0:
            return
        
        # 合并并去重
        combined_kline = current_kline.vstack(new_klines).unique(subset=["period_start"], keep="last")
        combined_kline = combined_kline.sort("period_start")
        if len(combined_kline) > kline_count:
            combined_kline = combined_kline.tail(kline_count)

        # 更新容器
        kline_holder.data = combined_kline
        self._save_kline_to_file(instrument_id, period, combined_kline)

    # K线持久化（双格式支持）
    def _save_kline_to_file(self, instrument_id: str, period: str, kline_df: polars.DataFrame) -> None:
        return
        """保存K线到文件"""
        file_path = self._get_kline_file_path(instrument_id, period)

        # 格式化时间字段
        formatted_df = kline_df.with_columns([
            polars.col("period_start").cast(polars.String).alias("period_start"),
            polars.col("period_end").cast(polars.String).alias("period_end"),
            polars.col("kline_start_time").cast(polars.String).alias("kline_start_time") 
            if "kline_start_time" in kline_df.columns else polars.lit(None),
            polars.col("kline_end_time").cast(polars.String).alias("kline_end_time")
            if "kline_end_time" in kline_df.columns else polars.lit(None)
        ])

        # 按格式保存
        if self.storage_format == "parquet":
            formatted_df.write_parquet(file_path)
        else:
            include_header = not os.path.exists(file_path)
            with open(file_path, "a", encoding="utf-8") as f:
                formatted_df.write_csv( f, include_header=include_header )

    # 数据获取接口
    def get_tick_holder(self, instrument_id: str) -> Optional[MutableTickHolder]:
        """获取Tick可变容器（外部通过容器获取最新数据）"""
        if instrument_id not in self.max_tick_count: self.max_tick_count[instrument_id] = 10000
        if instrument_id not in self.tick_cache:
            self._load_instrument_data(instrument_id)
        if instrument_id not in self.tick_cache: self.tick_cache[instrument_id] = MutableTickHolder()  
        type(self.tick_cache[instrument_id]),id(self.tick_cache[instrument_id]) 
        return self.tick_cache[instrument_id]#.tail(tick_count)

    def get_kline_holder(self, instrument_id: str, period: str, kline_count:int) -> Optional[MutableKlineHolder]:
        """获取K线可变容器"""
        if instrument_id not in self.max_kline_count: self.max_kline_count[instrument_id] = {period:kline_count}
        elif period not in self.max_kline_count[instrument_id]: self.max_kline_count[instrument_id][period] = kline_count
        else: self.max_kline_count[instrument_id][period] = max(self.max_kline_count[instrument_id][period], kline_count)
        if instrument_id not in self.max_tick_count: self.max_tick_count[instrument_id] = 10000
        if instrument_id in self.kline_cache and period in self.kline_cache[instrument_id]:
            return self.kline_cache[instrument_id][period]
        if not self._validate_period(period)[0]:
            print(f"无效周期：{period}")
            return None
        kline_holder = MutableKlineHolder()   #先创建空K线
        self.kline_cache[instrument_id] = {period:kline_holder}
        if instrument_id not in self.kline_periods: #缓存中无周期则添加
            self.kline_periods[instrument_id] = set()
        self.kline_periods[instrument_id].add(period)
        if instrument_id not in self.tick_cache: #缓存中无tick尝试从本地读取
            self._load_instrument_data(instrument_id)
        if instrument_id in self.tick_cache:
            # 生成K线
            kline_data = self.generate_any_period_kline(instrument_id, self.tick_cache[instrument_id].data, period, kline_count)
            kline_holder.data = kline_data
        else:
            tick_holder = MutableTickHolder() #创建空tick
            self.tick_cache[instrument_id] = tick_holder
        #kline =  self.kline_cache.get(instrument_id, {}).get(period)    
        return self.kline_cache[instrument_id][period]

    def _load_instrument_data(self, instrument_id: str) -> None:
        """懒加载指定合约数据"""
        tick_file = self._get_tick_file_path(instrument_id)
        if not os.path.exists(tick_file):
            print(f"未找到 {instrument_id} 的本地文件")
            return

        try:
            # 加载Tick
            tick_df = self._read_tick_file(tick_file)
            if instrument_id in self.tick_cache and isinstance(self.tick_cache[instrument_id],MutableTickHolder): self.tick_cache[instrument_id].data = tick_df
            else:
                tick_holder = MutableTickHolder()
                tick_holder.data = tick_df
                self.tick_cache[instrument_id] = tick_holder
            
            #print(f"懒加载 {instrument_id} 数据：{len(tick_df)} 条Tick")

        except Exception as e:
            print(f"懒加载 {instrument_id} 失败：{str(e)}")

    def get_latest_realtime_tick(self, instrument_id: str) -> Optional[polars.DataFrame]:
        """快捷获取最新Tick"""
        tick_holder = self.get_tick_holder(instrument_id)
        return tick_holder.tail(1) if tick_holder else None


