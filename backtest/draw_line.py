#!/usr/bin/env python
#  -*- coding: utf-8 -*-
import sys
import os
import subprocess
import multiprocessing
import threading
import time
from datetime import datetime
import polars as pl
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import webbrowser

# 🔥 全局配置：绝对路径+删除旧文件+Plotly浏览器常驻
pio.renderers.default = "browser"
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REALTIME_HTML_PATH = os.path.join(CURRENT_DIR, "plotly_realtime_backtest.html")
if os.path.exists(REALTIME_HTML_PATH):
    os.remove(REALTIME_HTML_PATH)


# -------------------------- 修复核心：RealTimeBalanceBuffer类（列数匹配+Polars1.8.2兼容） --------------------------
class RealTimeBalanceBuffer():
    """实时资金权益缓冲区（修复列数不匹配+Expr下标+去重语法，适配Polars1.8.2）"""
    def __init__(
        self,
        max_raw_points: int = 1000,
        sample_interval: int = 5,
        max_total_points: int = 5000
    ):
        self.max_raw_points = max_raw_points
        self.sample_interval = sample_interval
        self.max_total_points = max_total_points
        self.interval = 1000  # 默认刷新间隔（毫秒）
        self.backtest = ''
        self.buffer = pl.DataFrame({
                    "ctp_datetime": pl.Series([], dtype=pl.Datetime),
                    "Balance": pl.Series([], dtype=pl.Float64),
                    "PreBalance": pl.Series([], dtype=pl.Float64),  # 期初权益列
                    "risk_ratio": pl.Series([], dtype=pl.Float64)
                })

    def add_data(self, new_data: dict):
        new_df = pl.DataFrame([new_data])
        self.buffer = self.buffer.vstack(new_df)
        self._prune_data()

    def _prune_data(self):
        total_points = len(self.buffer)
        if total_points <= self.max_total_points:
            return
        latest_df = self.buffer.tail(self.max_raw_points)
        early_df = self.buffer.head(total_points - self.max_raw_points)
        if len(early_df) <= self.sample_interval:
            self.buffer = pl.concat([early_df, latest_df], how="vertical").sort("ctp_datetime")
            return
        
        early_df = early_df.with_row_index("row_idx")
        early_df = early_df.with_columns((pl.col("row_idx") // self.sample_interval).alias("bucket"))
        
        # 计算每个bucket内Balance极值的行索引
        early_df = early_df.with_columns(
            pl.col("Balance").arg_max().over("bucket").alias("max_balance_idx"),
            pl.col("Balance").arg_min().over("bucket").alias("min_balance_idx")
        )
        # 按bucket聚合首/尾行（保留4列，匹配buffer结构）
        first_rows = early_df.group_by("bucket").agg(
            pl.col("ctp_datetime").first(),
            pl.col("Balance").first(),
            pl.col("PreBalance").first(),
            pl.col("risk_ratio").first()
        ).drop("bucket")
        last_rows = early_df.group_by("bucket").agg(
            pl.col("ctp_datetime").last(),
            pl.col("Balance").last(),
            pl.col("PreBalance").last(),
            pl.col("risk_ratio").last()
        ).drop("bucket")
        # 筛选极值行（保留4列）
        max_balance = early_df.filter(
            pl.col("row_idx") == pl.col("max_balance_idx")
        ).select(["ctp_datetime", "Balance", "PreBalance", "risk_ratio"])
        min_balance = early_df.filter(
            pl.col("row_idx") == pl.col("min_balance_idx")
        ).select(["ctp_datetime", "Balance", "PreBalance", "risk_ratio"])
        
        # 拼接后去重，保证列数一致
        sampled_early = pl.concat([first_rows, last_rows, max_balance, min_balance], how="vertical")
        sampled_early = sampled_early.unique(subset=["ctp_datetime", "Balance"])
        self.buffer = pl.concat([sampled_early, latest_df], how="vertical").sort("ctp_datetime")
        if len(self.buffer) > self.max_total_points:
            self.buffer = self.buffer.tail(self.max_total_points)

    def get_data(self):
        return self.buffer.sort("ctp_datetime")

    def clear(self):
        self.buffer = self.buffer.head(0)

# -------------------------- 实时绘图：优化累计净值+动态y轴+收益标注 --------------------------
def plot_realtime_metrics(
    buffer: RealTimeBalanceBuffer,
    interval: int = 1000,
    max_display_points: int = 1000,
    figsize: tuple = (1400, 800)
):
    global CURRENT_DIR, REALTIME_HTML_PATH
    # 1. 定义HTML保存路径（优先当前目录，失败则切换到桌面）
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    REALTIME_HTML_PATH = os.path.join(CURRENT_DIR, "plotly_realtime_backtest.html")
    
    # 2. 校验当前目录是否可写，不可写则切换到用户桌面（兼容Windows）
    if not os.access(CURRENT_DIR, os.W_OK):
        print(f"⚠️ 当前目录[{CURRENT_DIR}]无写入权限，切换到桌面保存")
        if sys.platform == "win32":
            # Windows桌面路径
            CURRENT_DIR = os.path.join(os.environ.get("HOMEPATH", ""), "Desktop")
        else:
            # 其他系统桌面路径
            CURRENT_DIR = os.path.join(os.environ.get("HOME", ""), "Desktop")
        # 确保桌面目录存在（防止异常情况）
        os.makedirs(CURRENT_DIR, exist_ok=True)
        REALTIME_HTML_PATH = os.path.join(CURRENT_DIR, "plotly_realtime_backtest.html")
    
    # 3. 提前删除旧文件（避免残留）
    if os.path.exists(REALTIME_HTML_PATH):
        try:
            os.remove(REALTIME_HTML_PATH)
            print(f"✅ 已删除旧HTML文件：{REALTIME_HTML_PATH}")
        except Exception as e:
            print(f"⚠️ 删除旧文件失败：{str(e)}，继续生成新文件")
    
    
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=("账户资金权益", "累计净值（基准1.0）", "资金使用率（风控阈值0.8）"),
        vertical_spacing=0.15  # 加大间距，适配标注
    )
    fig.update_layout(
        width=figsize[0],
        height=figsize[1],
        title=dict(text="账户交易指标分析（实时回测）", font=dict(family="SimHei", size=16, weight="bold"), x=0.5, y=0.98),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(family="SimHei", size=10)),
        hovermode="x unified",
        margin=dict(l=50, r=50, t=80, b=80),
        plot_bgcolor="rgba(248,248,255,0.8)",
        paper_bgcolor="white"
    )
    # 添加绘图轨迹
    fig.add_trace(go.Scatter(x=[], y=[], mode="lines", line=dict(color="#1f77b4", width=2), name="资金权益(Balance)"), row=1, col=1)
    fig.add_trace(go.Scatter(x=[], y=[], mode="lines", line=dict(color="#ff7f0e", width=1.5), name="累计净值"), row=2, col=1)
    fig.add_trace(go.Scatter(x=[], y=[], mode="lines", line=dict(color="#2ca02c", width=1.5), name="资金使用率(risk_ratio)"), row=3, col=1)
    # 基础参考线
    fig.add_hline(y=1.0, line=dict(color="rgba(0,0,0,0.8)", dash="dash", width=1.2), row=2, col=1, name="净值基准线")
    fig.add_hline(y=0.8, line=dict(color="rgba(255,165,0,0.7)", dash="dash", width=1), row=3, col=1, name="风控安全阈值")
    fig.add_hline(y=0.0, line=dict(color="rgba(255,0,0,0.5)", dash="dash", width=1), row=3, col=1, name="使用率0轴")

    # 坐标轴配置（统一字体和精度）
    fig.update_xaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="时间", font=dict(family="SimHei", size=12)), tickfont=dict(family="SimHei", size=9), row=1, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="时间", font=dict(family="SimHei", size=12)), tickfont=dict(family="SimHei", size=9), row=2, col=1)
    fig.update_xaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="时间", font=dict(family="SimHei", size=12)), tickfont=dict(family="SimHei", size=9), row=3, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="资金权益（元）", font=dict(family="SimHei", size=12, color="#1f77b4")), tickfont=dict(family="SimHei", size=10), row=1, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="累计净值（4位小数）", font=dict(family="SimHei", size=12, color="#ff7f0e")), tickfont=dict(family="SimHei", size=10), tickformat=".4f", row=2, col=1)
    fig.update_yaxes(showgrid=True, gridcolor="lightgray", gridwidth=0.5, title=dict(text="资金使用率", font=dict(family="SimHei", size=12, color="#2ca02c")), tickfont=dict(family="SimHei", size=10), tickformat=".4f", row=3, col=1)

    try:
        # 同步写入初始HTML（阻塞主线程，直到文件生成完成）
        fig.write_html(
            REALTIME_HTML_PATH,
            include_plotlyjs="cdn",
            auto_open=False,
            full_html=True
        )
        print(f"✅ 初始HTML文件同步生成：{REALTIME_HTML_PATH}")
        # 同步添加刷新标签（文件已存在，无风险）
        refresh_tag = f'<meta http-equiv="refresh" content="{interval/1000}">'
        with open(REALTIME_HTML_PATH, "r+", encoding="utf-8") as f:
            html_content = f.read().replace("<head>", f"<head>{refresh_tag}")
            f.seek(0)
            f.write(html_content)
            f.truncate()
        print(f"✅ 刷新标签同步添加完成")
    except Exception as e:
        print(f"❌ 同步生成HTML失败：{str(e)}", flush=True)
        import traceback
        traceback.print_exc()
        return None, None
    
    
    try:
        browser_path = f"file:///{REALTIME_HTML_PATH.replace(os.sep, '/')}"
        if sys.platform == "win32":
            subprocess.Popen(f"start \"\" {browser_path}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.Popen(["xdg-open", browser_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"✅ 浏览器已启动，HTML路径：{REALTIME_HTML_PATH}")
    except Exception as e:
        print(f"❌ 启动浏览器失败：{str(e)}", flush=True)
    print(f"✅ 实时绘图HTML已生成：{REALTIME_HTML_PATH}")
    print(f"✅ Plotly实时绘图已在浏览器打开，自动刷新间隔{interval}ms")

    def update_plot():
        last_write_time = time.time()  # 新增：上一次写HTML的时间
        write_interval = 1.0  # 新增：HTML重写间隔（1秒/次）
        while True:
            if buffer.backtest is None:  # 回测结束标志，退出循环
                refresh_tag = f'<meta http-equiv="refresh" content="{interval/1000}">'
                with open(REALTIME_HTML_PATH, "r+", encoding="utf-8") as f:
                    html_content = f.read().replace('<meta http-equiv="refresh" content="1.0">', f"")
                    f.seek(0)
                    f.write(html_content)
                    f.truncate()
                return
            df = buffer.get_data()
            # 🔥 非关键步骤异步兜底：检查并添加刷新标签（不影响主流程）
            refresh_tag = f'<meta http-equiv="refresh" content="{buffer.interval/1000}">'
            if len(df) < 2:
                time.sleep(0.1)
                
                continue
            now = time.time()
            if now - last_write_time < write_interval:
                time.sleep(0.05)
                continue
            last_write_time = now
            # 数据预处理：PreBalance空值向前填充+累计净值计算（保留4位小数）
            df_clean = df.sort("ctp_datetime").with_columns(
                pl.col("PreBalance").fill_null(strategy="forward"),  # 空值向前填充，解决期初权益缺失
                (pl.col("Balance") / pl.col("PreBalance")).round(4).alias("cum_net_value")  # 累计净值保留4位小数
            )

            # 数据采样，避免点过多卡顿
            if len(df_clean) > max_display_points:
                sample_step = int(np.ceil(len(df_clean) / max_display_points))
                df_sample = df_clean.filter(
                    (pl.col("ctp_datetime").cum_count() % sample_step == 0) | 
                    (pl.col("ctp_datetime").cum_count() == len(df_clean)-1)
                )
            else:
                df_sample = df_clean
            # 提取数据并格式化
            times = [t.strftime("%Y-%m-%d %H:%M:%S") for t in df_sample["ctp_datetime"].to_list()]
            balance = df_sample["Balance"].to_list()
            cum_net_value = df_sample["cum_net_value"].to_list()
            risk_ratio = df_sample["risk_ratio"].to_list()

            # 更新曲线数据
            fig.data[0].x = times
            fig.data[0].y = balance
            fig.data[1].x = times
            fig.data[1].y = cum_net_value
            fig.data[2].x = times
            fig.data[2].y = risk_ratio

            # 🔥 优化1：累计净值动态y轴，自适应波动（上下留2%余量）
            if cum_net_value:
                min_nv = min(cum_net_value)
                max_nv = max(cum_net_value)
                # 若净值波动过小，固定y轴范围，避免抖动
                if max_nv - min_nv < 0.05:
                    fig.update_yaxes(range=[0.95, 1.05], row=2, col=1)
                else:
                    fig.update_yaxes(range=[min_nv*0.98, max_nv*1.02], row=2, col=1)

            # 🔥 优化2：资金使用率动态y轴+最大值参考线（每次更新重新绘制，避免重复）
            if risk_ratio:
                max_risk = max(risk_ratio)
                y_upper = max(0.85, max_risk * 1.1)
                fig.update_yaxes(range=[0, y_upper], row=3, col=1)
                # 清除旧的使用率最大值参考线，添加新的
                fig.for_each_annotation(lambda a: a.update(visible=False) if "最大使用率" in a.text else None)
                fig.add_annotation(
                    x=times[-1], y=max_risk, text=f"最大使用率：{max_risk:.4f}",
                    font=dict(family="SimHei", size=9, color="red"), showarrow=True, arrowhead=1, ax=0, ay=-20,
                    row=3, col=1, xref="x", yref="y"
                )

            # 🔥 优化3：添加累计净值实时标注（当前净值+累计收益率）
            fig.for_each_annotation(lambda a: a.update(visible=False) if "当前净值" in a.text or "累计收益率" in a.text else None)
            if cum_net_value:
                current_nv = cum_net_value[-1]
                current_return = (current_nv - 1.0) * 100  # 累计收益率（%）
                # 添加当前净值标注
                fig.add_annotation(
                    x=times[-1], y=current_nv, text=f"当前净值：{current_nv:.4f}",
                    font=dict(family="SimHei", size=9, color="#ff7f0e"), showarrow=True, arrowhead=1, ax=0, ay=-20,
                    row=2, col=1, xref="x", yref="y"
                )
                # 添加累计收益率标注（盈利绿色，亏损红色）
                return_color = "green" if current_return > 0 else "red"
                fig.add_annotation(
                    x=times[-1], y=current_nv, text=f"累计收益率：{current_return:.2f}%",
                    font=dict(family="SimHei", size=9, color=return_color), showarrow=True, arrowhead=1, ax=0, ay=-40,
                    row=2, col=1, xref="x", yref="y"
                )

            try:
                fig.write_html(REALTIME_HTML_PATH, include_plotlyjs="cdn", auto_open=False)
                #print(f"📤 HTML文件同步更新完成")
            except Exception as e:
                print(f"❌ 同步更新HTML失败：{str(e)}", flush=True)
                continue

            def check_refresh_tag():
                try:
                    with open(REALTIME_HTML_PATH, "r+", encoding="utf-8") as f:
                        html_content = f.read()
                        if "http-equiv=\"refresh\"" not in html_content:
                            html_content = html_content.replace("<head>", f"<head>{refresh_tag}")
                            f.seek(0)
                            f.write(html_content)
                            f.truncate()
                            #print(f"🔄 异步添加刷新标签完成")
                except Exception as e:
                    print(f"⚠️  异步检查刷新标签失败：{str(e)}", flush=True)
            
            # 启动异步线程执行非关键检查（不阻塞主流程）
            threading.Thread(target=check_refresh_tag, daemon=True).start()
            time.sleep(interval / 1000)
            

    update_thread = threading.Thread(target=update_plot, daemon=True)
    update_thread.start()
    return update_thread, fig

# -------------------------- 启动实时绘图入口 --------------------------
def start_realtime_animation(buffer: RealTimeBalanceBuffer,data_queue:multiprocessing.Queue, interval: int = 1000):
    update_thread, fig = plot_realtime_metrics(buffer, interval=interval)

    def consume_data():
        while True:
            try:
                # 批量消费：一次性取队列中所有数据 
                batch_data = []
                while not data_queue.empty()  :
                    d = data_queue.get(block=False)
                    if d is None:  # 约定：None表示无数据，跳过
                        buffer.interval = 3600*1000
                        buffer.backtest = None
                        return
                    batch_data.append(d)
                if not batch_data:  # 无数据则休眠，减少CPU空转
                    time.sleep(0.1)
                    continue
                # 批量处理：构造Polars DataFrame，一次性vstack，替代逐行add_data
                batch_df = pl.DataFrame(batch_data).with_columns(
                    pl.col("Balance").round(2),
                    pl.col("PreBalance").round(2),
                    pl.col("risk_ratio").round(4)
                )
                # 批量添加到缓冲区（一次vstack，替代多次单行vstack）
                buffer.buffer = buffer.buffer.vstack(batch_df )   
                buffer._prune_data()  # 仅批量处理后执行一次采样，减少计算
            except multiprocessing.Queue.Empty:
                time.sleep(0.01)
                continue
            except Exception as e:
                print(f"❌ 数据消费异常：{str(e)}", flush=True)
                continue

    consume_thread = threading.Thread(target=consume_data, daemon=True)
    consume_thread.start()
    return update_thread, consume_thread, fig

def plot_metrics_separately(
    df: pl.DataFrame,
    balance_col: str = "Balance",
    risk_ratio_col: str = "risk_ratio",
    time_col: str = "ctp_datetime",
    figsize: tuple = (1400, 800),
    max_display_points: int = 1000,
    save_html: bool = False,
    save_path: str = "./backtest_metrics_analysis.html"
):
    # 数据预处理：过滤时间+空值处理+累计净值计算（和实时绘图逻辑一致）
    df_clean = (
        df
        .filter(pl.col(time_col) >= datetime(2021,10,8,9,0,0))
        .drop_nulls(subset=[balance_col, risk_ratio_col, time_col])
        .sort(time_col)
        .with_columns(
            pl.col("PreBalance").fill_null(strategy="forward"),  # 期初权益空值向前填充
            (pl.col(balance_col) / pl.col("PreBalance")).round(4).alias("cum_net_value")  # 累计净值4位小数
        )
        .with_row_index("row_idx")
    )

    total_points = len(df_clean)
    if total_points < 2:
        raise ValueError("数据量不足，至少需要2个数据点")

    # 数据采样
    if total_points > max_display_points:
        sample_step = int(np.ceil(total_points / max_display_points))
        df_sample = (
            df_clean
            .filter((pl.col("row_idx") % sample_step == 0) | (pl.col("row_idx") == 0) | (pl.col("row_idx") == total_points - 1))
            .drop("row_idx")
        )
        print(f"⚠️  数据量过大（{total_points}个点），已采样至{len(df_sample)}个点")
    else:
        df_sample = df_clean.drop("row_idx")

    # 提取数据
    times = [t.strftime("%Y-%m-%d %H:%M:%S") for t in df_sample[time_col].to_list()]
    balance = df_sample[balance_col].to_list()
    cum_net_value = df_sample["cum_net_value"].to_list()
    risk_ratio = df_sample[risk_ratio_col].to_list()
    # 计算全局统计值
    current_nv = cum_net_value[-1] if cum_net_value else 1.0
    total_return = (current_nv - 1.0) * 100
    max_risk = max(risk_ratio) if risk_ratio else 0.0
    max_nv = max(cum_net_value) if cum_net_value else 1.0
    min_nv = min(cum_net_value) if cum_net_value else 1.0

    # 绘制静态图
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=(
            "账户资金权益", 
            f"累计净值（基准1.0，最高：{max_nv:.4f} | 最低：{min_nv:.4f}）", 
            f"资金使用率（风控阈值0.8，最大值：{max_risk:.4f}）"
        ),
        vertical_spacing=0.15
    )
    fig.update_layout(
        width=figsize[0],
        height=figsize[1],
        title=dict(
            text=f"账户交易指标分析（回测结果）| 最终净值：{current_nv:.4f} | 累计收益率：{total_return:.2f}%",
            font=dict(family="SimHei", size=16, weight="bold"), x=0.5, y=0.98
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(family="SimHei", size=10)),
        hovermode="x unified",
        margin=dict(l=50, r=50, t=80, b=80),
        plot_bgcolor="rgba(248,248,255,0.8)",
        paper_bgcolor="white"
    )

    # 1. 资金权益曲线
    fig.add_trace(
        go.Scatter(x=times, y=balance, mode="lines", line=dict(color="#1f77b4", width=2),
                   name="资金权益(Balance)", hovertemplate="<b>时间</b>：%{x}<br><b>资金权益</b>：%{y:.2f} 元<extra></extra>"),
        row=1, col=1
    )
    fig.update_yaxes(
        title=dict(text="资金权益（元）", font=dict(family="SimHei", size=12, color="#1f77b4")),
        tickfont=dict(family="SimHei", size=10), showgrid=True, gridcolor="lightgray", gridwidth=0.5,
        row=1, col=1
    )

    # 2. 累计净值曲线（4位小数+动态y轴+标注）
    fig.add_trace(
        go.Scatter(x=times, y=cum_net_value, mode="lines", line=dict(color="#ff7f0e", width=1.5),
                   name="累计净值", hovertemplate="<b>时间</b>：%{x}<br><b>累计净值</b>：%{y:.4f}<extra></extra>"),
        row=2, col=1
    )
    fig.add_hline(y=1.0, line=dict(color="rgba(0,0,0,0.8)", dash="dash", width=1.2), row=2, col=1)
    # 净值极值标注
    fig.add_annotation(
        x=times[cum_net_value.index(max_nv)], y=max_nv, text=f"最高净值：{max_nv:.4f}",
        font=dict(family="SimHei", size=9, color="#ff7f0e"), showarrow=True, arrowhead=1, ax=0, ay=-20,
        row=2, col=1
    )
    fig.add_annotation(
        x=times[cum_net_value.index(min_nv)], y=min_nv, text=f"最低净值：{min_nv:.4f}",
        font=dict(family="SimHei", size=9, color="#ff7f0e"), showarrow=True, arrowhead=1, ax=0, ay=20,
        row=2, col=1
    )
    # 最终净值+收益率标注
    return_color = "green" if total_return > 0 else "red"
    fig.add_annotation(
        x=times[-1], y=current_nv, text=f"最终净值：{current_nv:.4f}<br>累计收益率：{total_return:.2f}%",
        font=dict(family="SimHei", size=9, color=return_color), showarrow=True, arrowhead=1, ax=0, ay=-30,
        row=2, col=1
    )
    fig.update_yaxes(
        title=dict(text="累计净值（4位小数）", font=dict(family="SimHei", size=12, color="#ff7f0e")),
        tickfont=dict(family="SimHei", size=10), tickformat=".4f", showgrid=True, gridcolor="lightgray", gridwidth=0.5,
        row=2, col=1
    )

    # 3. 资金使用率曲线
    fig.add_trace(
        go.Scatter(x=times, y=risk_ratio, mode="lines", line=dict(color="#2ca02c", width=1.5),
                   name="资金使用率(risk_ratio)", hovertemplate="<b>时间</b>：%{x}<br><b>资金使用率</b>：%{y:.4f}<extra></extra>"),
        row=3, col=1
    )
    fig.add_hline(y=0.8, line=dict(color="rgba(255,165,0,0.7)", dash="dash", width=1), row=3, col=1)
    fig.add_hline(y=max_risk, line=dict(color="rgba(255,0,0,0.5)", dash="dash", width=1), row=3, col=1)
    fig.add_annotation(
        x=times[risk_ratio.index(max_risk)], y=max_risk, text=f"最大使用率：{max_risk:.4f}",
        font=dict(family="SimHei", size=9, color="red"), showarrow=True, arrowhead=1, ax=0, ay=-20,
        row=3, col=1
    )
    fig.update_yaxes(
        title=dict(text="资金使用率", font=dict(family="SimHei", size=12, color="#2ca02c")),
        tickfont=dict(family="SimHei", size=10), tickformat=".4f", showgrid=True, gridcolor="lightgray", gridwidth=0.5,
        row=3, col=1
    )

    # 统一x轴配置
    for i in range(1, 4):
        fig.update_xaxes(
            title=dict(text="时间", font=dict(family="SimHei", size=12)),
            tickfont=dict(family="SimHei", size=9), tickangle=0,
            showgrid=True, gridcolor="lightgray", gridwidth=0.5,
            row=i, col=1
        )

    # 显示并保存
    fig.show(renderer="browser", block=True)
    if save_html:
        fig.write_html(save_path, include_plotlyjs="cdn")
        print(f"✅ 回测分析报告已保存为HTML：{os.path.abspath(save_path)}")

def draw_main(data_queue:multiprocessing.Queue):
    """绘图子进程入口：所有绘图逻辑在此执行，独立于回测进程"""
    try:
        # 核心：所有绘图相关导入移到函数内部（避免子进程重加载）
        import sys
        import os
        current_path = os.path.abspath(__file__)
        root_path = os.path.abspath(os.path.join(current_path, "../../../"))
        if root_path not in sys.path:
            sys.path.insert(0, root_path)
        #from . import RealTimeBalanceBuffer, start_realtime_animation  # 相对导入，不触发包初始化
        import multiprocessing
        import time
        
        # 初始化绘图缓冲区（和原逻辑一致）
        plot_buffer = RealTimeBalanceBuffer(
            max_raw_points=1000,
            sample_interval=5,
            max_total_points=5000
        )
        # 启动实时绘图
        plotter_thread, consume_thread, realtime_fig = start_realtime_animation(
            plot_buffer, data_queue,
            interval=1000
        )
        print("✅ 实时绘图线程已启动")
        # 让子进程持续运行
        while True:
            time.sleep(1)
    except Exception as e:
        print(f"❌ 绘图子进程初始化失败：{str(e)}", flush=True)
        import traceback
        traceback.print_exc()  # 打印详细堆栈，方便调试
# -------------------------- 主程序测试（可选） --------------------------
if __name__ == "__main__":
    try:
        sample_data = pl.read_csv(fr'C:\datas\backtest_result.csv', try_parse_dates=True)
        plot_metrics_separately(
            sample_data,
            max_display_points=1000,
            save_html=True,
            save_path="./回测指标分析报告_优化版.html"
        )
        while True:
            time.sleep(10)
    except FileNotFoundError:
        print(f"⚠️  本地回测数据文件未找到，启动实时绘图测试！")
        buffer = RealTimeBalanceBuffer()
        start_realtime_animation(buffer)
        while True:
            # 模拟数据：资金权益小幅波动，期初权益固定，资金使用率低波动
            buffer.add_data({
                "ctp_datetime": datetime.now(),
                "Balance": 1000000 + np.random.randint(-800, 1200),  # 权益在100万左右波动
                "PreBalance": 1000000,  # 全局期初权益固定
                "risk_ratio": round(np.random.uniform(0.01, 0.06), 4)  # 使用率1%-6%
            })
            time.sleep(1)