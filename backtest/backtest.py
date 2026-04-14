#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
# 获取当前脚本的绝对路径
current_path = os.path.abspath(__file__)
# 每调用一次 os.path.dirname() 就向上一层
libs_dir = os.path.dirname(current_path)  # 上层：libs 目录
peoplequant_dir = os.path.dirname(libs_dir)      # 再上层：PeopleQuant 目录
parent_dir = os.path.dirname(peoplequant_dir)    # 再上层：6.7.11 目录（目标父目录）
# 将根目录添加到 sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
import polars as pl

import numpy as np
import time as tm

import threading
from typing import Dict, Optional,Tuple,List, Generator
import re
import gc
from datetime import datetime, time, date, timedelta,timezone
from peoplequant.zhustruct import Quote, Account, Position, Order, Trade


class BackTest():
    def __init__(self,filepaths:list,start_datetime:datetime,end_datetime:datetime,balance:float=1000000.0,symbol_infos={},
                 col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "LastPrice": "close", "AskPrice1": "", "BidPrice1": "", 
                 "AskVolume1": "", "BidVolume1": "", "Volume": "volume","OpenInterest":"OpenInterest"},datetime_format = "%Y-%m-%d %H:%M:%S%.f",result_file:str='',draw_line=None,
                 chunk_size=1000):
        '''
        filepaths:品种数据路径列表,可传入tick、K线数据,由此数据生成行情快照
        start_datetime:回测开始时间,最早为数据开始时间
        end_datetime:回测结束时间,最晚为数据结束时间
        balance:回测资金
        symbol_infos：品种基础属性,交易所ExchangeID，价格最小跳PriceTick，合约乘数VolumeMultiple，开仓保证金率OpenRatioByMoney，每手手续费OpenRatioByVolume，
                      涨跌停幅度LimitPriceRatio
                      symbol_infos = {
                            'rb2610': {"ExchangeID": "SHFE", 'PriceTick':1, "VolumeMultiple":10, "OpenRatioByMoney":0.1, "OpenRatioByVolume":10,"LimitPriceRatio":0.1},
                            'm2610': {"ExchangeID": "DCE", 'PriceTick':1, "VolumeMultiple":10, "OpenRatioByMoney":0.1, "OpenRatioByVolume":10,"LimitPriceRatio":0.1},
                            }
        col_mapping:本地数据的字段名称,例如：日期datetime、合约码symbol、最新价close、成交量volume，若本地数据为tick，则可再传入卖一价、买一价、卖一量、买一量的名称，
                    若本地数据为K线，盘口买卖报价默认为最新价加减1跳，成交量暂无意义。,OpenInterest持仓量
        datetime_format:日期datetime的格式
        result_file:回测结果保存目录
        draw_line:是否启用浏览器实时绘制资金曲线
        '''
        self.filepaths = filepaths
        self.quote_filepaths = [f if '_quote' in f[:-4] else f"{f[:-4]}_quote.{f[-3:]}" for f in filepaths]
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.balance = balance
        self.symbol_infos = symbol_infos
        self.col_mapping = col_mapping  # 新增：保存列映射，供后续生成器使用
        self.datetime_format = datetime_format  # 新增：保存时间格式，供后续生成器使用
        self.chunk_size = chunk_size  # 新增：保存分块大小，供后续生成器使用
        pl.Config.set_streaming_chunk_size(self.chunk_size)  # 全局配置分块大小
        if result_file:
            self.result_file = result_file
        else:
            try:
                _flowfile = os.path.dirname(os.path.abspath(__file__)) #当前程序目录(该py文件__file__所在目录)
            except: _flowfile = os.getcwd()
            self.result_file = fr'{_flowfile}\logs'
        print('等待回测数据初始化')
        self.data_dfs = []
        for filepath, quote_filepath in zip(self.filepaths, self.quote_filepaths):
            if not os.path.isfile(quote_filepath):
                #df_quote = read_file_to_tick(filepath,symbol_infos,col_mapping=col_mapping,datetime_format=datetime_format)
                #df_quote.write_csv(quote_filepath)
                read_file_to_tick_and_save( filepath=filepath, output_path=quote_filepath, symbol_infos=symbol_infos, col_mapping=col_mapping, datetime_format=datetime_format )
            # 核心：调用轻量首行方法，仅读取文件第一行，不加载全量
            df = self.read_file_to_tick_head1(
                (filepath, quote_filepath),
                symbol_infos,
                col_mapping=col_mapping,
                datetime_format=datetime_format
            )
            # 过滤时间范围（仅1行数据，计算极快，无性能损耗）
            #df = df.filter((pl.col("datetime") >= pl.lit(start_datetime)) & (pl.col("datetime") <= pl.lit(end_datetime)))
            self.data_dfs.append(df)
        #for filepath in filepaths:
        #    df = read_file_to_tick(filepath,symbol_infos,col_mapping=col_mapping,datetime_format=datetime_format)
        #    df = df.filter((pl.col("datetime") >= pl.lit(start_datetime)) & (pl.col("datetime") <= pl.lit(end_datetime)))
        #    self.data_dfs.append(df)
        #self.union_time = get_union_time_index(self.data_dfs)
        #self.quote_dfs = ((Quote().update(q).to_dict() for q in align_to_union_time(df, self.union_time).to_dicts()) for df in self.data_dfs)
        self.union_time = self._get_lightweight_union_time()
        self.data_lazy_gens = self.create_lazy_data_generators()
        self.quote_dfs = (self._stream_quote_generator(lazy_gen) for lazy_gen in self.data_lazy_gens)
        
        self.draw_line = draw_line
        self.last_push_time = datetime.now()  # 新增：上一次推送绘图数据的时间
        self.push_interval = 0.5  # 新增：推送频率限制（500ms/次）
        self.plotter_thread = None  # 置空：绘图子进程内部管理线程
        self.consume_thread = None
        self.realtime_fig = None
        self.draw_process = None  # 新增：保存绘图子进程实例，方便后续关闭
        if draw_line:
            try:
                import multiprocessing
                from peoplequant.backtest import draw_line  # 导入绘图模块
                data_queue = multiprocessing.Queue(maxsize=0)
                self.data_queue = data_queue
                # 启动绘图子进程
                self.draw_process = multiprocessing.Process(
                    target=draw_line.draw_main,args=(data_queue,),
                    daemon=True,
                    name="PlotSubProcess"
                )
                self.draw_process.start()
                print(f"✅ 绘图子进程已启动，进程ID：{self.draw_process.pid}")
            except Exception as e:
                print(f"⚠️  实时绘图模块初始化失败：{str(e)}，跳过绘图")
                self.draw_line = False  # 初始化失败则关闭绘图
                self.plotter_thread = None
                self.consume_thread = None
                self.qt_app = None

    # 新增：回测主线程保活方法（在BackTest类中）
    def keep_main_thread_running(self):
        """仅做绘图保活标记，不再启动子线程执行exec_()，交给主线程处理"""
        self.qt_app_keep_running = True  # 新增标记，标识需要主线程执行exec_()
        print("✅ 绘图主线程保活标记已设置，Qt事件循环将在主线程执行")

    def calculate_risk_metrics(self,
        df: pl.DataFrame,
        balance_col: str = "Balance",
        time_col = 'ctp_datetime',
        risk_free_rate: float = 0.03,  # 无风险利率（年化3%）
        annualization_factor: int = 240  # 年化因子（交易日数，股票252，期货240）
    ) -> dict:
        """
        基于资金权益列计算夏普比率、索提诺比率、最大回撤
        :param df: 包含Balance和时间列的DataFrame（需按时间升序排列）
        :param balance_col: 资金权益列名
        :param time_col: 时间列名
        :param risk_free_rate: 年化无风险利率（默认3%）
        :param annualization_factor: 年化因子（默认252个交易日）
        :return: 包含三个指标的字典
        """
        # 步骤1：数据预处理（确保按时间排序，计算日收益率）
        df_clean = (
            df
            # 移除Balance为空的行
            .drop_nulls(subset=[balance_col])
            .unique(subset=["TradingDay"],keep='last')
            .sort(time_col)
            # 计算日收益率 = (当日权益 - 前日权益) / 前日权益
            .with_columns([
                pl.col(balance_col).pct_change().alias("daily_return"),
                # 计算累计权益峰值（用于最大回撤）
                pl.col(balance_col).cum_max().alias("peak_balance")
            ])
            # 移除收益率为空的第一行
            .drop_nulls(subset=["daily_return"])
        )
        
        if len(df_clean) < 2:
            print("数据量不足,无法计算评价指标,至少需要2个权益数据点")
            return {}
        
        # 步骤2：计算基础统计量
        # 日收益率均值
        daily_return_mean = df_clean["daily_return"].mean()
        # 日收益率标准差（夏普比率用）
        daily_return_std = df_clean["daily_return"].std()
        # 下行风险（索提诺比率用：仅计算负收益率的标准差）
        downside_returns = df_clean.filter(pl.col("daily_return") < 0)["daily_return"]
        downside_risk = downside_returns.std() if len(downside_returns) > 0 else 0.0
        
        # 步骤3：计算年化超额收益（无风险利率按日折算）
        daily_risk_free_rate = (1 + risk_free_rate) ** (1 / annualization_factor) - 1
        excess_daily_return_mean = daily_return_mean - daily_risk_free_rate
        
        # 步骤4：计算夏普比率（年化）
        sharpe_ratio = 0.0
        if daily_return_std != 0:
            sharpe_ratio = excess_daily_return_mean * np.sqrt(annualization_factor) / daily_return_std
        
        # 步骤5：计算索提诺比率（年化）
        sortino_ratio = 0.0
        if downside_risk != 0:
            sortino_ratio = excess_daily_return_mean * np.sqrt(annualization_factor) / downside_risk
        
        # 步骤6：计算最大回撤（百分比，负数表示亏损）
        df_clean = df_clean.with_columns([
            # 回撤 = (当前权益 / 累计峰值) - 1
            (pl.col(balance_col) / pl.col("peak_balance") - 1).alias("drawdown")
        ])
        max_drawdown = df_clean["drawdown"].min()  # 最小回撤值即最大亏损幅度
        
        # 步骤7：整理结果（保留4位小数，更易读）
        return {
            "夏普率": round(sharpe_ratio, 4),
            "索提诺比率": round(sortino_ratio, 4),
            "最大回撤": round(max_drawdown, 4),
            #"daily_return_mean": round(daily_return_mean, 6),
            #"annual_return": round((df_clean[balance_col].last() / df_clean[balance_col].first() - 1) * 100, 2)  # 总收益率（%）
        }

    def finish_backtest(self,df:pl.DataFrame):
        """回测结束后执行，生成绘图分析报告"""
        from peoplequant.backtest import draw_line
        # 获取缓冲区全量数据（Polars DataFrame）
        #df = self.plot_buffer.get_data()
        if len(df) < 2:
            print("⚠️  绘图缓冲区数据量不足，无法生成分析报告")
            return
        # 生成Plotly交互式分析报告（浏览器打开，不自动关闭）
        draw_line.plot_metrics_separately(
            df=df,
            max_display_points=2000,  # 可根据数据量调整
            save_html=True,
            save_path=f"{self.result_file}\回测指标分析报告.html"  # 保存到回测日志目录
        )
        #print(f"✅ 回测分析报告已生成：{self.result_file}\回测指标分析报告.html")

    def push_draw_data(self, current_datetime, current_balance, pre_balance, current_risk_ratio):
        """回测数据推送统一方法，添加频率限制"""
        if not self.draw_line:  # 关闭绘图直接返回
            return
        if current_datetime is None:  # 无时间数据，不绘图
            self.data_queue.put(None)
            return
        # 频率限制：未到间隔时间，直接跳过推送
        now = datetime.now()
        if (now - self.last_push_time).total_seconds() < self.push_interval:
            return
        self.last_push_time = now  # 更新最后推送时间

        if hasattr(self, "data_queue"):
            try:
                self.data_queue.put({
                    "ctp_datetime": current_datetime,
                    "Balance": round(current_balance, 2),
                    "PreBalance": round(pre_balance, 2),
                    "risk_ratio": round(current_risk_ratio, 4)
                }, block=False)  # 新增block=False：即使无界队列，也强制非阻塞
            except Exception as e:
                print(f"⚠️  绘图数据推送失败：{str(e)}")
    # 🔥 新增：在BackTest类中添加回测结束时的绘图线程停止方法
    def stop_draw_thread(self):
        """回测结束时优雅停止绘图线程"""
        if hasattr(self, "plotter_thread") and self.plotter_thread is not None:
            try:
                self.plotter_thread.stop()
            except Exception as e:
                print(f"⚠️  停止绘图线程失败：{str(e)}")

    def _get_lightweight_union_time(self) -> pl.DataFrame:
        """轻量生成联合时间轴：仅读取所有文件的datetime列，不加载任何行情数据"""
        time_dfs = []
        for filepath in self.quote_filepaths:
            # 利用原有read_only_datetime，仅加载datetime列，超轻量
            df_time = read_only_datetime(
                filepath=filepath,
                col_mapping=self.col_mapping,
                datetime_format=self.datetime_format
            )
            # 过滤回测时间范围，减少时间轴数据量
            df_time = df_time.filter(
                (pl.col("datetime") >= pl.lit(self.start_datetime)) & 
                (pl.col("datetime") <= pl.lit(self.end_datetime))
            )
            time_dfs.append(df_time)
        # 生成联合时间轴（仅datetime列）
        union_time = get_union_time_index(time_dfs)
        # 释放临时时间列内存
        del time_dfs
        return union_time
    
    def create_lazy_data_generators(self) -> Generator[pl.LazyFrame, None, None]:
        """创建单品种的懒加载查询计划生成器，按需执行，不占内存"""
        for filepath in self.quote_filepaths:
            # 1. 懒加载CSV：pl.scan_csv替代pl.read_csv，仅生成查询计划，不加载数据
            lazy_df = pl.scan_csv(filepath,schema_overrides={"TradingDay":pl.String,"ActionDay":pl.String})
            if lazy_df.collect_schema()["datetime"] != pl.Datetime:
                lazy_df = lazy_df.with_columns(
                    pl.col("datetime").str.strptime(pl.Datetime, self.datetime_format).alias("datetime")
                )
            
            # 3. 过滤回测时间范围（懒加载过滤，仅执行时生效）
            lazy_df = lazy_df.filter(
                (pl.col("datetime") >= pl.lit(self.start_datetime)) & 
                (pl.col("datetime") <= pl.lit(self.end_datetime))
            )
            # 生成器：迭代时才执行上述所有懒加载逻辑
            yield lazy_df

    def _create_lazy_data_generators(self) -> Generator[pl.LazyFrame, None, None]:
        """创建单品种的懒加载查询计划生成器，按需执行，不占内存"""
        for filepath in self.filepaths:
            # 1. 懒加载CSV：pl.scan_csv替代pl.read_csv，仅生成查询计划，不加载数据
            lazy_df = pl.scan_csv(filepath)
            # 2. 应用read_file_to_tick的所有逻辑（改造为懒加载版）
            lazy_df = self._lazy_read_file_to_tick(
                lazy_df=lazy_df,
                symbol_infos=self.symbol_infos,
                col_mapping=self.col_mapping,
                datetime_format=self.datetime_format
            )
            # 3. 过滤回测时间范围（懒加载过滤，仅执行时生效）
            lazy_df = lazy_df.filter(
                (pl.col("datetime") >= pl.lit(self.start_datetime)) & 
                (pl.col("datetime") <= pl.lit(self.end_datetime))
            )
            # 生成器：迭代时才执行上述所有懒加载逻辑
            yield lazy_df

    def _lazy_read_file_to_tick(self, lazy_df: pl.LazyFrame, symbol_infos:dict, 
                            col_mapping: Dict[str, str], datetime_format: str) -> pl.LazyFrame:
        """懒加载版read_file_to_tick：所有操作均为查询计划，不实际加载数据"""
        # 1. 重命名列（懒加载）
        rename_map = {v:k for k,v in col_mapping.items() if v}
        lazy_df = lazy_df.rename(rename_map)
        # 2. 获取品种信息（先执行一次取symbol，仅加载一行，超轻量）
        symbol = lazy_df.select("InstrumentID").first().collect().item()
        symbol_info = symbol_infos.get(symbol, {'PriceTick':1, 'LimitPriceRatio':0.1})
        # 3. 补全盘口数据（懒加载）
        columns = lazy_df.collect_schema().names()  # 仅获取列名，不加载数据
        if "AskPrice1" not in columns or "BidPrice1" not in columns:
            lazy_df = lazy_df.with_columns([
                (pl.col("LastPrice") + symbol_info['PriceTick']).alias("AskPrice1"),
                (pl.col("LastPrice") - symbol_info['PriceTick']).alias("BidPrice1"),
                (pl.col("LastPrice")*(1 + symbol_info['LimitPriceRatio'])).alias("UpperLimitPrice"),
                (pl.col("LastPrice")*(1 - symbol_info['LimitPriceRatio'])).alias("LowerLimitPrice"),
                pl.lit(0).alias("BandingUpperPrice"),
                pl.lit(0).alias("BandingLowerPrice"),
                pl.lit(0).alias("AskVolume1"),
                pl.lit(0).alias("BidVolume1")
            ])
        # 4. 转换datetime类型（懒加载）
        if lazy_df.collect_schema()["datetime"] != pl.Datetime:
            lazy_df = lazy_df.with_columns(
                pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
            )
        # 5. 补全ActionDay/TradingDay等时间列（懒加载，原有逻辑完全保留）
        lazy_df = lazy_df.with_columns([
            pl.col("datetime").dt.strftime("%Y%m%d").alias("ActionDay"),
            pl.when(pl.col("datetime").dt.hour() >= 19)
            .then(pl.when(pl.col("datetime").dt.weekday() == 5)
                .then(pl.col("datetime").dt.offset_by("3d"))
                .otherwise(pl.col("datetime").dt.offset_by("1d")))
            .when(pl.col("datetime").dt.hour() < 8)
            .then(pl.when(pl.col("datetime").dt.weekday() == 6)
                .then(pl.col("datetime").dt.offset_by("2d"))
                .otherwise(pl.col("datetime")))
            .otherwise(pl.col("datetime"))
            .dt.strftime("%Y%m%d").alias("TradingDay"),
            pl.col("datetime").dt.strftime("%H:%M:%S").alias("UpdateTime"),
            pl.col("datetime").dt.millisecond().alias("UpdateMillisec"),
        ])
        # 6. 选择指定列（懒加载）
        select_cols = list(col_mapping.keys()) + ["ActionDay","TradingDay","UpdateTime","UpdateMillisec",
                                                "UpperLimitPrice","LowerLimitPrice","BandingUpperPrice","BandingLowerPrice"]
        lazy_df = lazy_df.select(select_cols)
        return lazy_df
    
    def _stream_quote_generator(self, lazy_df: pl.LazyFrame) -> Generator[dict, None, None]:
        """流式行情生成器：懒加载对齐时间轴，逐行生成Quote字典，生成后释放内存"""
        union_time_lazy = self.union_time.lazy()
        # 1. 将懒加载查询计划执行为【流式DataFrame】（polars的streaming模式，分块处理）
        aligned_lazy = union_time_lazy.join(lazy_df, on="datetime", how="left").sort("datetime")
        # 开启streaming模式，分块处理大数据，块大小可根据内存调整（如1000行/块）
        stream_df = aligned_lazy.collect(streaming=True)
        
        # 2. 逐行迭代流式DataFrame，生成Quote对象
        for row in stream_df.to_dicts():
            # 生成Quote字典（与原有逻辑一致）
            quote_dict = row # Quote().update(row).to_dict()
            yield quote_dict
        # 3. 迭代结束后，强制释放当前块的内存
        del stream_df
        
        gc.collect()  # 手动触发垃圾回收，确保内存释放
    def read_file_to_tick_head1(self,
        filepath,
        symbol_infos:dict,
        col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "LastPrice": "close", "AskPrice1": "", "BidPrice1": "", 
                                    "AskVolume1": "", "BidVolume1": "", "Volume": "volume","OpenInterest":"OpenInterest"}, 
        datetime_format = "%Y-%m-%d %H:%M:%S%.f"
    ) -> pl.DataFrame:
        """
        轻量读取文件**仅第一行**并转换为tick格式，不加载全量数据，彻底降低内存占用
        完全复用read_file_to_tick的业务逻辑，仅数据加载层改为head(1)
        """
        # 核心：懒加载+仅读取首行，不加载全量数据（Polars底层仅读取文件首行，无全量IO）
        if os.path.isfile(filepath[1]):
            lazy_df = pl.scan_csv(filepath[1],schema_overrides={"TradingDay":pl.String,"ActionDay":pl.String})  # 生成懒加载查询计划，不实际读取
            if lazy_df.collect_schema()["datetime"] != pl.Datetime:
                lazy_df = lazy_df.with_columns(
                    pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
                )
                lazy_df = lazy_df.filter(
                (pl.col("datetime") >= pl.lit(self.start_datetime)) &
                (pl.col("datetime") <= pl.lit(self.end_datetime)) &
                (pl.col("InstrumentID") != '')
            )
            
            df = lazy_df.head(1).collect()
            return df
        else:
            lazy_df = pl.scan_csv(filepath[0])  # 生成懒加载查询计划，不实际读取
            #df = lazy_df.head(1).collect()   # 仅执行首行读取，内存占用≈单行数据大小
            rename_map = {v: k for k, v in col_mapping.items() if v}
            lazy_df = lazy_df.rename(rename_map)
            if "datetime" in lazy_df.collect_schema().names():
                lazy_df = lazy_df.with_columns(
                    pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
                )
            lazy_df = lazy_df.filter(
                (pl.col("datetime") >= pl.lit(self.start_datetime)) &
                (pl.col("datetime") <= pl.lit(self.end_datetime)) &
                (pl.col("InstrumentID") != '')
            )
            
            df = lazy_df.head(1).collect()
            if df.is_empty():
                return df
            # 以下逻辑与原read_file_to_tick完全一致，无任何修改
            #df = df.rename({v:k for k,v in col_mapping.items() if v})
            # 处理空文件/首行无数据的情况
            #if df.is_empty():
            #    return pl.DataFrame(schema=df.schema)
            symbol = df["InstrumentID"][0]
            symbol_info = symbol_infos.get(symbol,{'PriceTick':1, 'LimitPriceRatio':0.1})
            columns = df.columns
            if "AskPrice1" not in columns or "BidPrice1" not in columns:
                df = df.with_columns(
                    (pl.col("LastPrice") + symbol_info['PriceTick']).alias("AskPrice1"),
                    (pl.col("LastPrice") - symbol_info['PriceTick']).alias("BidPrice1"), 
                    (pl.col("LastPrice")*(1 + symbol_info['LimitPriceRatio'])).alias("UpperLimitPrice"),
                    (pl.col("LastPrice")*(1 - symbol_info['LimitPriceRatio'])).alias("LowerLimitPrice"),
                    pl.lit(0).alias("BandingUpperPrice"),
                    pl.lit(0).alias("BandingLowerPrice"),
                    pl.lit(0).alias("AskVolume1"),
                    pl.lit(0).alias("BidVolume1")
                )
            #if  not isinstance(df["datetime"].dtype, pl.Datetime):
            #    df = df.with_columns(
            #        pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
            #    )
            df = df.with_columns(
                        pl.col("datetime").dt.strftime("%Y%m%d").alias("ActionDay"),
                        pl.when(pl.col("datetime").dt.hour() >= 19)
                        .then(
                            pl.when(pl.col("datetime").dt.weekday() == 5)
                            .then(pl.col("datetime").dt.offset_by("3d"))
                            .otherwise(pl.col("datetime").dt.offset_by("1d"))
                        )
                        .when(pl.col("datetime").dt.hour() < 8)
                        .then(
                            pl.when(pl.col("datetime").dt.weekday() == 6)
                            .then(pl.col("datetime").dt.offset_by("2d"))
                            .otherwise(pl.col("datetime"))
                        )
                        .otherwise(pl.col("datetime"))
                        .dt.strftime("%Y%m%d").alias("TradingDay"),
                        pl.col("datetime").dt.strftime("%H:%M:%S").alias("UpdateTime"),
                        pl.col("datetime").dt.millisecond().alias("UpdateMillisec"),
                    )
            return df.select(list(col_mapping.keys())+["ActionDay","TradingDay","UpdateTime","UpdateMillisec","UpperLimitPrice","LowerLimitPrice","BandingUpperPrice","BandingLowerPrice"])

def read_kline_file(filepath,col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "open": "open", "high": "high", "low": "low", 
                                                          "close": "close", "Volume": "volume","OpenInterest":"OpenInterest"}, 
                    datetime_format = "%Y-%m-%d %H:%M:%S%.f") -> pl.DataFrame:
    data = pl.read_csv(filepath,
                    #try_parse_dates = True
                    #,parse_dates=["datetime" ]
                    )
    # 重命名列
    data = data.rename({v:k for k,v in col_mapping.items()})
    columns = data.columns
    if  not isinstance(data["datetime"].dtype, pl.Datetime):
        data = data.with_columns(
            pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
        )
    if "trading_day" in columns and not isinstance(data["trading_day"].dtype, pl.Date):
                data = data.with_columns(
                    pl.col("trading_day").str.strptime(pl.Date, "%Y-%m-%d").alias("trading_day")
                )
    elif "trading_day" not in columns:
        data = data.with_columns(
                    pl.col("datetime").cast(pl.Date).alias("trading_day")
                )
    return data.select([
        "datetime", "trading_day", "InstrumentID", "open", "high", "low", "close", "Volume"
    ])

def read_file_to_tick(filepath,symbol_infos:dict,col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "LastPrice": "close", "AskPrice1": "", "BidPrice1": "", 
                                                                        "AskVolume1": "", "BidVolume1": "", "Volume": "volume","OpenInterest":"OpenInterest"}, 
                    datetime_format = "%Y-%m-%d %H:%M:%S%.f") -> pl.DataFrame:
    data = pl.read_csv(filepath,
                    #try_parse_dates = True
                    #,parse_dates=["datetime" ]
                    )
    # 重命名列
    data = data.rename({v:k for k,v in col_mapping.items() if v})
    symbol = data["InstrumentID"][0]
    symbol_info = symbol_infos.get(symbol,{'PriceTick':1})
    columns = data.columns
    if "AskPrice1" not in columns or "BidPrice1" not in columns:
        data = data.with_columns(
            (pl.col("LastPrice") + symbol_info['PriceTick']).alias("AskPrice1"),
            (pl.col("LastPrice") - symbol_info['PriceTick']).alias("BidPrice1"), 
            (pl.col("LastPrice")*(1 + symbol_info['LimitPriceRatio'])).alias("UpperLimitPrice"),
            (pl.col("LastPrice")*(1 - symbol_info['LimitPriceRatio'])).alias("LowerLimitPrice"),
            pl.lit(0).alias("BandingUpperPrice"),
            pl.lit(0).alias("BandingLowerPrice"),
            pl.lit(0).alias("AskVolume1"),
            pl.lit(0).alias("BidVolume1")
        )
    if  not isinstance(data["datetime"].dtype, pl.Datetime):
        data = data.with_columns(
            pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
        )
    data = data.sort("datetime").with_columns(
                pl.col("datetime").dt.strftime("%Y%m%d").alias("ActionDay"),
                # 🔥 重构TradingDay计算逻辑：分19点后、8点前、其余时间三层判断
                pl.when(pl.col("datetime").dt.hour() >= 19)
                # 第一层：19:00后 → 周五+3天，其他+1天
                .then(
                    pl.when(pl.col("datetime").dt.weekday() == 5)  # 5=周五（Polars中weekday:1=周一，7=周日）
                    .then(pl.col("datetime").dt.offset_by("3d"))
                    .otherwise(pl.col("datetime").dt.offset_by("1d"))
                )
                # 第二层：8:00前 → 周六+2天，其他不偏移
                .when(pl.col("datetime").dt.hour() < 8)
                .then(
                    pl.when(pl.col("datetime").dt.weekday() == 6)  # 6=周六
                    .then(pl.col("datetime").dt.offset_by("2d"))
                    .otherwise(pl.col("datetime"))
                )
                # 第三层：8:00-16:00 → 保持原日期
                .otherwise(pl.col("datetime"))
                .dt.strftime("%Y%m%d").alias("TradingDay"),
                # 原时间列（不变）
                pl.col("datetime").dt.strftime("%H:%M:%S").alias("UpdateTime"),
                pl.col("datetime").dt.millisecond().alias("UpdateMillisec"),
            )
    #if "trading_day" in columns and not isinstance(data["trading_day"].dtype, pl.Date):
    #            data = data.with_columns(
    #                pl.col("trading_day").str.strptime(pl.Date, "%Y-%m-%d").alias("trading_day")
    #            )
    #elif "trading_day" not in columns:
    #    data = data.with_columns(
    #                pl.col("datetime").cast(pl.Date).alias("trading_day")
    #            )
    return data.select(list(col_mapping.keys())+["ActionDay","TradingDay","UpdateTime","UpdateMillisec","UpperLimitPrice","LowerLimitPrice","BandingUpperPrice","BandingLowerPrice"])

def read_file_to_tick_and_save(
    filepath,
    output_path,
    symbol_infos: dict,
    col_mapping: Dict[str, str],
    datetime_format="%Y-%m-%d %H:%M:%S%.f",
    chunk_size=100000  # 分块大小，可改
):
    """
    手动分块读取大文件 → 处理 → 追加写入，彻底解决schema不匹配问题
    内存只占用1个chunk，不会爆
    """
    # 1. 先读取首行，获取symbol信息
    df_head = pl.read_csv(filepath, n_rows=1)
    rename_map = {v: k for k, v in col_mapping.items() if v}
    df_head = df_head.rename(rename_map)
    symbol = df_head["InstrumentID"][0]
    symbol_info = symbol_infos.get(symbol, {'PriceTick': 1, 'LimitPriceRatio': 0.1})
    del df_head

    # 2. 分块读取器（手动分块，最稳）
    reader = pl.read_csv_batched(
        filepath,
        batch_size=chunk_size
    )

    # 3. 逐块处理 + 逐块写入（第一次写header，之后append）
    first_write = True
    while True:
        try:
            chunk = reader.next_batches(1)
            if chunk is None or len(chunk) == 0:
                break
            df = chunk[0]

            # ----------------------
            # 你的原有处理逻辑（完全不变）
            # ----------------------
            df = df.rename({v: k for k, v in col_mapping.items() if v})

            if "AskPrice1" not in df.columns or "BidPrice1" not in df.columns:
                df = df.with_columns(
                    (pl.col("LastPrice") + symbol_info['PriceTick']).alias("AskPrice1"),
                    (pl.col("LastPrice") - symbol_info['PriceTick']).alias("BidPrice1"),
                    (pl.col("LastPrice")*(1+symbol_info['LimitPriceRatio'])).alias("UpperLimitPrice"),
                    (pl.col("LastPrice")*(1-symbol_info['LimitPriceRatio'])).alias("LowerLimitPrice"),
                    pl.lit(0).alias("BandingUpperPrice"),
                    pl.lit(0).alias("BandingLowerPrice"),
                    pl.lit(0).alias("AskVolume1"),
                    pl.lit(0).alias("BidVolume1")
                )

            if not isinstance(df["datetime"].dtype, pl.Datetime):
                df = df.with_columns(
                    pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
                )

            df = df.sort("datetime").with_columns(
                pl.col("datetime").dt.strftime("%Y%m%d").alias("ActionDay"),
                pl.when(pl.col("datetime").dt.hour()>=19).then(
                    pl.when(pl.col("datetime").dt.weekday()==5).then(pl.col("datetime").dt.offset_by("3d"))
                    .otherwise(pl.col("datetime").dt.offset_by("1d"))
                ).when(pl.col("datetime").dt.hour()<8).then(
                    pl.when(pl.col("datetime").dt.weekday()==6).then(pl.col("datetime").dt.offset_by("2d"))
                    .otherwise(pl.col("datetime"))
                ).otherwise(pl.col("datetime")).dt.strftime("%Y%m%d").alias("TradingDay"),
                pl.col("datetime").dt.strftime("%H:%M:%S").alias("UpdateTime"),
                pl.col("datetime").dt.millisecond().alias("UpdateMillisec")
            )

            select_cols = list(col_mapping.keys()) + [
                "ActionDay","TradingDay","UpdateTime","UpdateMillisec",
                "UpperLimitPrice","LowerLimitPrice","BandingUpperPrice","BandingLowerPrice"
            ]
            df = df.select(select_cols)

            # 写入文件（第一行带header，之后append）
            if first_write:
                df.write_csv(output_path, include_header=True,datetime_format=datetime_format)
                first_write = False
            else:
                # 追加：用with open(mode='a')，无header
                with open(output_path, mode='a', newline='', encoding='utf-8') as f:
                    df.write_csv(f, include_header=False,datetime_format=datetime_format)

            # 释放块内存
            del df

        except StopIteration:
            break

    # 强制回收
    import gc
    gc.collect()

def standardize_tick_df(df:pl.DataFrame,symbol_info,col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "LastPrice": "close", "AskPrice1": "", "BidPrice1": "", "AskVolume1": "", 
                                                                                 "BidVolume1": "", "Volume": "volume","OpenInterest":"OpenInterest"}, 
                    datetime_format = "%Y-%m-%d %H:%M:%S%.f") -> pl.DataFrame:
    # 重命名列
    df = df.rename({v:k for k,v in col_mapping.items() if v})
    columns = df.columns
    if "AskPrice1" not in columns or "BidPrice1" not in columns:
        df = df.with_columns(
            (pl.col("LastPrice") + symbol_info['PriceTick']).alias("AskPrice1"),
            (pl.col("LastPrice") - symbol_info['PriceTick']).alias("BidPrice1"), 
            (pl.col("LastPrice")*(1 + symbol_info['LimitPriceRatio'])).alias("UpperLimitPrice"),
            (pl.col("LastPrice")*(1 - symbol_info['LimitPriceRatio'])).alias("LowerLimitPrice"),
            pl.lit(0).alias("BandingUpperPrice"),
            pl.lit(0).alias("BandingLowerPrice"),
            pl.lit(0).alias("AskVolume1"),
            pl.lit(0).alias("BidVolume1")
        )
    if  not isinstance(df["datetime"].dtype, pl.Datetime):
        df = df.with_columns(
            pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
        )
    df = df.sort("datetime").with_columns(
            pl.col("datetime").dt.strftime("%Y%m%d").alias("ActionDay"),
            # 计算 TradingDay：16:00 前用当天，16:00 后用次日
            pl.when(pl.col("datetime").dt.hour() >= 16)
            .then(pl.col("datetime").dt.offset_by("1d"))
            .otherwise(pl.col("datetime"))
            .dt.strftime("%Y%m%d").alias("TradingDay"),
            pl.col("datetime").dt.strftime("%H:%M:%S").alias("UpdateTime"),
            pl.col("datetime").dt.millisecond().alias("UpdateMillisec"),
        )
    #if "trading_day" in columns and not isinstance(df["trading_day"].dtype, pl.Date):
    #            df = df.with_columns(
    #                pl.col("trading_day").str.strptime(pl.Date, "%Y-%m-%d").alias("trading_day")
    #            )
    #elif "trading_day" not in columns:
    #    df = df.with_columns(
    #                pl.col("datetime").cast(pl.Date).alias("trading_day")
    #            )
    return df.select(list(col_mapping.keys())+["ActionDay","TradingDay","UpdateTime","UpdateMillisec","UpperLimitPrice","LowerLimitPrice","BandingUpperPrice","BandingLowerPrice"])

def standardize_kline_df(
    df: pl.DataFrame,
    col_mapping: Dict[str, str]={"datetime": "datetime","InstrumentID": "symbol", "open": "open", "high": "high", "low": "low", 
                                 "close": "close", "Volume": "volume","OpenInterest":"OpenInterest"},
    datetime_col: str = "datetime",
    symbol_col: Optional[str] = None,
    datetime_format = "%Y-%m-%d %H:%M:%S%.f"
) -> pl.DataFrame:
    """
    标准化原始 K 线数据为统一格式
    
    Parameters:
        df: 原始 DataFrame
        col_mapping: 映射原始列名到标准列名，必须包含：
            {"datetime": "...", "open": "...", "high": "...", "low": "...", "close": "...", "volume": "..."}
        datetime_col: 用于提取 trading_day 的时间列（通常是 col_mapping["datetime"]）
        symbol_col: 品种列名（可选，若无则设为常量）
    
    Returns:
        标准化后的 DataFrame，包含列：
        symbol, datetime, trading_day, open, high, low, close, volume
    """
    # 重命名列
    df_std = df.rename({v:k for k,v in col_mapping.items()})
    
    # 添加 symbol 列
    if symbol_col is None:
        df_std = df_std.with_columns(pl.lit("UNKNOWN").alias("InstrumentID"))
    else:
        if symbol_col != "InstrumentID":
            df_std = df_std.rename({symbol_col: "InstrumentID"})
    
    # 确保 datetime 是 Datetime 类型
    if df_std["datetime"].dtype != pl.Datetime:
        df_std = df_std.with_columns(pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime"))
    
    # 自动添加 trading_day（从 datetime 提取日期）
    if "trading_day" not in df_std.columns:
        df_std = df_std.with_columns(
            pl.col("datetime").cast(pl.Date).alias("trading_day")
        )
    else:
        # 若已有，确保是 Date 类型
        if df_std["trading_day"].dtype != pl.Date:
            df_std = df_std.with_columns(pl.col("trading_day").str.to_date())
    
    # 保证列顺序
    return df_std.select([
        "datetime", "trading_day", "InstrumentID", "open", "high", "low", "close", "Volume"
    ])

def resample_kline_by_symbol(
    df: pl.DataFrame,
    timeframe: str,
    ohlc_cols: Dict[str, str] = None
) -> pl.DataFrame:
    """
    按 InstrumentID 分组合成高周期 K 线
    """
    if ohlc_cols is None:
        ohlc_cols = {"open": "open", "high": "high", "low": "low", "close": "close", "Volume": "Volume"}
    
    o, h, l, c, v = ohlc_cols["open"], ohlc_cols["high"], ohlc_cols["low"], ohlc_cols["close"], ohlc_cols["Volume"]
    datetime_col = "datetime"
    trading_day_col = "trading_day"
    
    is_intraday = not any(unit in timeframe for unit in ["d", "w", "mo"])
    
    if is_intraday:
        # 日内：直接 group_by_dynamic + by=InstrumentID
        result = (
            df.sort(datetime_col)
            .group_by_dynamic(
                index_column=datetime_col,
                every=timeframe,
                closed="both",
                label="left",
                group_by="InstrumentID"
            )
            .agg([
                pl.col(trading_day_col).first(),
                pl.col(o).first().alias("open"),
                pl.col(h).max().alias("high"),
                pl.col(l).min().alias("low"),
                pl.col(c).last().alias("close"),
                pl.col(v).sum().alias("Volume")
            ])
        )
    else:
        # 日及以上：先聚合成日线（保留 InstrumentID）
        daily = (
            df.sort("InstrumentID", datetime_col)
            .group_by("InstrumentID", trading_day_col)
            .agg([
                pl.col(o).first().alias("open"),
                pl.col(h).max().alias("high"),
                pl.col(l).min().alias("low"),
                pl.col(c).last().alias("close"),
                pl.col(v).sum().alias("Volume")
            ])
        )
        # 将 trading_day 转为 Datetime 作为索引
        daily = daily.with_columns(
            pl.col(trading_day_col).cast(pl.Datetime).alias("_dt")
        )
        # 再用 group_by_dynamic + by=InstrumentID 重采样（周/月）
        result = (
            daily
            .sort("_dt")
            .group_by_dynamic(
                index_column="_dt",
                every=timeframe,
                closed="left",
                label="left",
                by="InstrumentID"  # 👈 关键：保留 InstrumentID
            )
            .agg([
                pl.col(trading_day_col).first(),
                pl.col("open").first(),
                pl.col("high").max(),
                pl.col("low").min(),
                pl.col("close").last(),
                pl.col("Volume").sum()
            ])
            .with_columns(pl.col("_dt").alias(datetime_col))
            .drop("_dt")
        )
    
    return result.select([
        "datetime", "trading_day", "InstrumentID", "open", "high", "low", "close", "Volume"
    ]).sort("InstrumentID", datetime_col)


def _parse_timeframe_to_duration(timeframe: str) -> pl.Expr:
    """
    将 timeframe 字符串（如 '5m', '1h'）转换为 Polars duration 表达式
    """
    tf = timeframe.strip().lower()
    if tf.endswith('m'):
        return pl.duration(minutes=int(tf[:-1]))
    elif tf.endswith('h'):
        return pl.duration(hours=int(tf[:-1]))
    elif tf.endswith('d'):
        return pl.duration(days=int(tf[:-1]))
    elif tf.endswith('w'):
        return pl.duration(weeks=int(tf[:-1]))
    elif tf.endswith('mo'):
        return pl.duration(months=int(tf[:-2]))
    else:
        raise ValueError(f"Unsupported timeframe format: {timeframe}")

def join_higher_timeframe(
    df_low: pl.DataFrame,
    df_high: pl.DataFrame,
    timeframe: str,
    timeframe_suffix: str
) -> pl.DataFrame:
    """
    安全拼接高周期 K 线到低频数据（避免前视偏差）
    
    假设：
    - df_high 的 datetime 是 K 线的 *开始时间*（如 resample_kline_by_symbol 使用 label="left"）
    - 高周期 K 线只有在 *下一个周期开始时* 才可用（即 start_time + timeframe）
    
    例如：5m K线 [09:30, 09:35) 的完整数据，最早在 09:35 可用
    
    Parameters:
        df_low: 低频数据（如 1m）
        df_high: 高周期 K 线（如 5m），datetime = 窗口开始时间
        timeframe: 高周期字符串，如 "5m"
        timeframe_suffix: 列名后缀，如 "5m"
    
    Returns:
        拼接后的 DataFrame，高周期列在可用时间之前为 null
    """
    # 1. 重命名高周期列
    cols_to_rename = ["open", "high", "low", "close", "Volume"]
    rename_map = {col: f"{col}_{timeframe_suffix}" for col in cols_to_rename}
    df_high_renamed = df_high.rename(rename_map).select([
        "InstrumentID", "datetime", *[f"{col}_{timeframe_suffix}" for col in cols_to_rename]
    ])
    
    # 2. 计算高周期 K 线的“最早可用时间” = 开始时间 + 周期
    available_time_expr = pl.col("datetime") + _parse_timeframe_to_duration(timeframe)
    df_high_with_available = df_high_renamed.with_columns(
        available_time_expr.alias("available_at")
    ).drop("datetime")  # 不再需要原始时间戳
    
    # 3. 将低频数据的 datetime 作为左键，与 available_at 对齐
    df_low_with_key = df_low.with_columns(pl.col("datetime").alias("join_key"))
    
    # 4. left join：只有当低频时间 >= available_at 时，才匹配到高周期数据
    df_joined = df_low_with_key.join(
        df_high_with_available,
        left_on=["InstrumentID", "join_key"],
        right_on=["InstrumentID", "available_at"],
        how="left"
    ).drop("join_key")
    
    return df_joined

def align_multi_timeframe_for_symbol(
    df_base: pl.DataFrame,
    higher_klines: List[Tuple[pl.DataFrame, str, str]],  # (df_high, timeframe, suffix)
    symbol: str
) -> pl.DataFrame:
    """
    为单个品种拼接多周期数据
    
    Parameters:
        df_base: 基础数据（如 tick 或 1m）
        higher_klines: [(df_5m, "5m", "5m"), (df_15m, "15m", "15m"), ...]
        symbol: 品种代码
    """
    df = df_base.with_columns(pl.lit(symbol).alias("symbol"))
    
    for df_high, timeframe, suffix in higher_klines:
        df = join_higher_timeframe(
            df_low=df,
            df_high=df_high,
            timeframe=timeframe,
            timeframe_suffix=suffix
        )
    return df

def merge_symbols_into_wide_table(
    symbol_dfs: List[pl.DataFrame],
    time_col: str = "datetime",
    day_col: str = "trading_day"
) -> pl.DataFrame:
    """
    将多个品种的 DataFrame 横向拼接成宽表
    
    - 每个 df 必须包含: symbol, datetime, trading_day, ...
    - 拼接键: (trading_day, datetime)
    - 不同品种的列自动加前缀（如 RB2501_close, M2501_close_10m）
    - 缺失时间点填充 null
    """
    if not symbol_dfs:
        raise ValueError("No symbol data provided")
    
    # 1. 为每个品种的列加上 symbol 前缀（除公共键外）
    processed_dfs = []
    for df in symbol_dfs:
        # 提取 symbol（假设每张表只含一个品种）
        sym = df.select("symbol").unique().item()
        
        # 重命名非键列
        cols_to_rename = [col for col in df.columns if col not in ["trading_day", "datetime"]]
        rename_map = {col: f"{sym}_{col}" for col in cols_to_rename}
        df_renamed = df.rename(rename_map)
        
        # 保留键列不变
        df_final = df_renamed.select([
            "trading_day",
            "datetime",
            *[f"{sym}_{col}" for col in cols_to_rename]
        ])
        processed_dfs.append(df_final)
    
    # 2. 逐步 full join 所有品种
    result = processed_dfs[0]
    for df_other in processed_dfs[1:]:
        result = result.join(
            df_other,
            on=["trading_day", "datetime"],
            how="full",  # 保留所有时间点
            coalesce=True  # 合并 trading_day/datetime 的 null
        )
    
    # 3. 排序
    return result.sort(["trading_day", "datetime"])

def read_only_datetime(
    filepath: str,
    col_mapping: Dict[str, str] = {"datetime": "datetime"},
    datetime_format: str = "%Y-%m-%d %H:%M:%S%.f"
) -> pl.DataFrame:
    """
    轻量读取CSV仅保留datetime列，用于生成全局时间轴，不加载全量数据
    :return: 仅含datetime列的Polars DataFrame（已转Datetime类型）
    """
    # 仅读取映射后的datetime列，跳过所有其他列
    datetime_ori_col = [v for k, v in col_mapping.items() if k == "datetime"][0]
    # 只加载datetime列，大幅减少内存
    lazy_df = pl.scan_csv(filepath)
    if "datetime" in lazy_df.collect_schema().names():
        lazy_df = lazy_df.select(["datetime"])
    else:
        # 重命名
        lazy_df = lazy_df.select([datetime_ori_col]).rename({datetime_ori_col: "datetime"})
        #转类型
    df = lazy_df.collect()
    if not isinstance(df["datetime"].dtype, pl.Datetime):
        df = df.with_columns(
            pl.col("datetime").str.strptime(pl.Datetime, datetime_format).alias("datetime")
        )
    return df

def get_union_time_index(dfs: List[pl.DataFrame]) -> pl.DataFrame:
    """返回包含 datetime 和 trading_day 的联合时间轴"""
    union_dt = pl.concat([df["datetime"] for df in dfs], how="vertical").unique().sort()
    return union_dt.to_frame("datetime")#.with_columns( pl.col("datetime").cast(pl.Date).alias("trading_day") )

def align_to_union_time(df: pl.DataFrame, union_time: pl.DataFrame) -> pl.DataFrame:
    '''
    # 假设 rb_df 和 m_df 已有 datetime (Datetime) 和 trading_day (Date)
    union_time = get_union_time_index([rb_df, m_df])
    rb_aligned = align_to_union_time(rb_df, union_time)
    m_aligned = align_to_union_time(m_df, union_time)
    '''
    return union_time.join(df, on=[  "datetime"], how="left").sort("datetime")


def add_ma_signal(
    df: pl.DataFrame,
    price_col: str,
    short_window: int = 5,
    long_window: int = 20,
    signal_prefix: str = ""
) -> pl.DataFrame:
    """
    计算均线金叉死叉信号
    
    Returns:
        新增列: {prefix}_ma_short, {prefix}_ma_long, {prefix}_golden_cross, {prefix}_death_cross
    """
    prefix = signal_prefix + "_" if signal_prefix else ""
    
    df_with_ma = df.with_columns([
        pl.col(price_col).rolling_mean(window_size=short_window).alias(f"{prefix}ma{short_window}"),
        pl.col(price_col).rolling_mean(window_size=long_window).alias(f"{prefix}ma{long_window}")
    ])
    
    golden = (
        (pl.col(f"{prefix}ma{short_window}") > pl.col(f"{prefix}ma{long_window}")) &
        (pl.col(f"{prefix}ma{short_window}").shift(1) <= pl.col(f"{prefix}ma{long_window}").shift(1))
    )
    death = (
        (pl.col(f"{prefix}ma{short_window}") < pl.col(f"{prefix}ma{long_window}")) &
        (pl.col(f"{prefix}ma{short_window}").shift(1) >= pl.col(f"{prefix}ma{long_window}").shift(1))
    )
    
    return df_with_ma.with_columns([
        golden.alias(f"{prefix}golden_cross"),
        death.alias(f"{prefix}death_cross")
    ])




