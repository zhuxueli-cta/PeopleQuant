import polars as pl
import numpy as np
from typing import Union, Dict,Tuple, Literal

def _expr(column: str | pl.Expr) -> pl.Expr:
    """辅助函数：统一处理字符串或表达式输入"""
    return pl.col(column) if isinstance(column, str) else column


# =============================================================================
# 🧩 多周期对齐工具函数
# =============================================================================

def align_multi_timeframes(
    base_df: pl.DataFrame,
    higher_tf_dfs: Dict[str, pl.DataFrame],
    datetime_col: str = "datetime",
) -> pl.DataFrame:
    """
    将多个高周期 K 线对齐到基础（最高频）时间轴上。
    
    Args:
        base_df: 基础 DataFrame（如 1 分钟 K 线），必须包含 datetime_col
        higher_tf_dfs: 字典，如 {"1h": df_1h, "1d": df_daily}
        datetime_col: 时间列名
        
    Returns:
        合并后的 DataFrame，低频缺失值为 null
        
    Usage:
        df_merged = align_multi_timeframes(
            base_df=df_1m,
            higher_tf_dfs={"1h": df_1h, "1d": df_daily},
            datetime_col="datetime"
        )
    """
    df = base_df.clone()
    for tf_name, tf_df in higher_tf_dfs.items():
        if df[datetime_col].dtype != tf_df[datetime_col].dtype:
            tf_df = tf_df.with_columns(pl.col(datetime_col).cast(df[datetime_col].dtype))
        df = df.join(tf_df, on=datetime_col, how="left", suffix=f"_{tf_name}")
    return df


def add_signals_on_real_klines(
    df: pl.DataFrame,
    signal_expr: pl.Expr,
    timeframe_col: str,
    output_col: str = "signal"
) -> pl.DataFrame:
    """
    仅在真实 K 线上计算信号（非 null 处）
    
    Args:
        df: 合并对齐后的 DataFrame
        signal_expr: 信号表达式，如 (pl.col("close_1h") > pl.col("ma_1h"))
        timeframe_col: 高周期价格列，如 "close_1h"
        output_col: 输出信号列名
        
    Returns:
        带信号列的 DataFrame，非真实 K 线处为 null
        
    Usage:
        df = add_signals_on_real_klines(
            df_merged,
            signal_expr=(pl.col("close_1h") > pl.col("trma_1h")),
            timeframe_col="close_1h",
            output_col="long_signal"
        )
    """
    return df.with_columns(
        pl.when(pl.col(timeframe_col).is_not_null())
          .then(signal_expr)
          .otherwise(None)
          .alias(output_col)
    )


# =============================================================================
# 📈 核心指标函数（带 skip_nan 支持）
# =============================================================================

# ---------------- MA / SMA ----------------
def MA_expr(column: str | pl.Expr, window: int, skip_nan: bool = True) -> pl.Expr:
    """
    简单移动平均（SMA）
    
    Args:
        column: 价格列名或表达式
        window: 窗口大小
        skip_nan: 是否跳过空值（True=只要有1个有效值就计算）
        
    Returns:
        移动平均序列表达式
        
    Usage:
        df.with_columns(MA_expr("close", 20, skip_nan=True).alias("ma20"))
    """
    col = _expr(column)
    min_samples = 1 if skip_nan else window
    return col.rolling_mean(window_size=window, min_samples=min_samples)

def MA_df(df: pl.DataFrame, window: int, column: str = "close", output_name: str = "ma", skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 MA 列
    
    Usage:
        df = MA_df(df, window=20, column="close", output_name="ma20")
    """
    return df.with_columns(MA_expr(column, window, skip_nan).alias(output_name))

SMA_expr = MA_expr
SMA_df = MA_df


# ---------------- EMA ----------------
def EMA_expr(column: str | pl.Expr, span: int) -> pl.Expr:
    """
    指数移动平均（EMA），天然处理 NaN（无需 skip_nan）
    
    Args:
        column: 价格列名或表达式
        span: 平滑窗口（span ≈ 2/(N+1)）
        
    Returns:
        EMA 序列表达式
        
    Usage:
        df.with_columns(EMA_expr("close", 12).alias("ema12"))
    """
    return _expr(column).ewm_mean(span=span, adjust=False)

def EMA_df(df: pl.DataFrame, span: int, column: str = "close", output_name: str = "ema") -> pl.DataFrame:
    """
    在 DataFrame 上添加 EMA 列
    
    Usage:
        df = EMA_df(df, span=12, column="close", output_name="ema12")
    """
    return df.with_columns(EMA_expr(column, span).alias(output_name))


# ---------------- MACD ----------------
def MACD_expr(
    column: str | pl.Expr = "close",
    short: int = 12,
    long: int = 26,
    signal: int = 9,
    output_name: str = ""
) -> pl.Expr:
    """
    MACD 指标（基于 EMA，天然处理 NaN）
    
    Returns:
        struct{diff, dea, bar}
        
    Usage:
        df = df.with_columns(MACD_expr("close", 12, 26, 9).alias("macd")).unnest("macd")
    """
    price = _expr(column)
    diff = EMA_expr(price, short) - EMA_expr(price, long)
    dea = diff.ewm_mean(span=signal, adjust=False)
    bar = (diff - dea) * 2
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{f"{prefix}diff": diff, f"{prefix}dea": dea, f"{prefix}bar": bar})

def MACD_df(df: pl.DataFrame, short: int = 12, long: int = 26, signal: int = 9, column: str = "close", output_name: str = "") -> pl.DataFrame:
    """
    在 DataFrame 上添加 MACD 三列
    
    Usage:
        df = MACD_df(df, short=12, long=26, signal=9, output_name="macd")
        # 生成列: macd_diff, macd_dea, macd_bar
    """
    return df.with_columns(MACD_expr(column, short, long, signal, output_name).alias("macd")).unnest("macd")


# ---------------- BOLL (布林带) ----------------
def BOLL_expr(
    column: str | pl.Expr = "close",
    n: int = 20,
    p: float = 2.0,
    output_name: str = "",
    skip_nan: bool = True
) -> pl.Expr:
    """
    布林带指标
    
    Args:
        n: 中轨窗口
        p: 标准差倍数
        skip_nan: 是否跳过空值
        
    Returns:
        struct{mid, top, bottom}
        
    Usage:
        df = df.with_columns(BOLL_expr("close", 20, 2.0).alias("boll")).unnest("boll")
    """
    price = _expr(column)
    min_samples = 1 if skip_nan else n
    mid = price.rolling_mean(window_size=n, min_samples=min_samples)
    std = price.rolling_std(window_size=n, ddof=0, min_samples=min_samples)
    top = mid + p * std
    bottom = mid - p * std
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{f"{prefix}mid": mid, f"{prefix}top": top, f"{prefix}bottom": bottom})

def BOLL_df(df: pl.DataFrame, n: int = 20, p: float = 2.0, column: str = "close", output_name: str = "", skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加布林带三列
    
    Usage:
        df = BOLL_df(df, n=20, p=2.0, output_name="boll")
        # 生成列: boll_mid, boll_top, boll_bottom
    """
    return df.with_columns(BOLL_expr(column, n, p, output_name, skip_nan).alias("boll")).unnest("boll")


# ---------------- RSI ----------------
def RSI_expr(column: str | pl.Expr = "close", window: int = 14) -> pl.Expr:
    """
    相对强弱指数（RSI），基于 EMA，天然处理 NaN
    
    Usage:
        df.with_columns(RSI_expr("close", 14).alias("rsi"))
    """
    price = _expr(column)
    delta = price.diff()
    up = delta.clip(0, None)
    down = (-delta).clip(0, None)
    ema_up = up.ewm_mean(span=window, adjust=False)
    ema_down = down.ewm_mean(span=window, adjust=False)
    rs = pl.when(ema_down == 0).then(float('inf')).otherwise(ema_up / ema_down)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fill_null(100.0).fill_nan(100.0)

def RSI_df(df: pl.DataFrame, window: int = 14, column: str = "close", output_name: str = "rsi") -> pl.DataFrame:
    """
    在 DataFrame 上添加 RSI 列
    
    Usage:
        df = RSI_df(df, window=14, output_name="rsi14")
    """
    return df.with_columns(RSI_expr(column, window).alias(output_name))


# ---------------- ATR ----------------
def ATR_expr(window: int = 14, skip_nan: bool = True) -> pl.Expr:
    """
    平均真实波幅（ATR）
    
    Usage:
        df.with_columns(ATR_expr(14, skip_nan=True).alias("atr"))
    """
    hl = pl.col("high") - pl.col("low")
    hc = (pl.col("high") - pl.col("close").shift(1)).abs()
    lc = (pl.col("low") - pl.col("close").shift(1)).abs()
    tr = pl.max_horizontal([hl, hc, lc])
    min_samples = 1 if skip_nan else window
    return tr.rolling_mean(window_size=window, min_samples=min_samples)

def ATR_df(df: pl.DataFrame, window: int = 14, output_name: str = "atr", skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 ATR 列
    
    Usage:
        df = ATR_df(df, window=14, output_name="atr14")
    """
    return df.with_columns(ATR_expr(window, skip_nan).alias(output_name))


# ---------------- KDJ ----------------
def KDJ_expr(n: int = 9, m1: int = 3, m2: int = 3, output_name: str = '', skip_nan: bool = True) -> pl.Expr:
    """
    随机指标 KDJ
    
    Args:
        n: RSV 周期
        m1: K 平滑参数
        m2: D 平滑参数
        skip_nan: 是否跳过空值
        
    Returns:
        struct{k, d, j}
        
    Usage:
        df = df.with_columns(KDJ_expr(9, 3, 3).alias("kdj")).unnest("kdj")
    """
    min_samples = 1 if skip_nan else n
    hv = pl.col("high").rolling_max(window_size=n, min_samples=min_samples)
    lv = pl.col("low").rolling_min(window_size=n, min_samples=min_samples)
    rsv = pl.when(hv == lv).then(0.0).otherwise((pl.col("close") - lv) / (hv - lv) * 100)
    k = rsv.ewm_mean(alpha=m1 / n, adjust=False)
    d = k.ewm_mean(alpha=m2 / m1, adjust=False)
    j = 3 * k - 2 * d
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{f"{prefix}k": k, f"{prefix}d": d, f"{prefix}j": j})

def KDJ_df(df: pl.DataFrame, n: int = 9, m1: int = 3, m2: int = 3, output_name: str = '', skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 KDJ 三列
    
    Usage:
        df = KDJ_df(df, n=9, m1=3, m2=3, output_name="kdj")
        # 生成列: kdj_k, kdj_d, kdj_j
    """
    return df.with_columns(KDJ_expr(n, m1, m2, output_name, skip_nan).alias("kdj")).unnest("kdj")


# ---------------- CCI ----------------
def CCI_expr(window: int = 20, skip_nan: bool = True) -> pl.Expr:
    """
    顺势指标（CCI）
    
    Usage:
        df.with_columns(CCI_expr(20).alias("cci"))
    """
    typ = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    min_samples = 1 if skip_nan else window
    ma_typ = typ.rolling_mean(window_size=window, min_samples=min_samples)
    std_typ = typ.rolling_std(window_size=window, ddof=0, min_samples=min_samples)
    return (typ - ma_typ) / (0.015 * std_typ)

def CCI_df(df: pl.DataFrame, window: int = 20, output_name: str = "cci", skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 CCI 列
    
    Usage:
        df = CCI_df(df, window=20, output_name="cci20")
    """
    return df.with_columns(CCI_expr(window, skip_nan).alias(output_name))


# ---------------- OBV ----------------
def OBV_expr() -> pl.Expr:
    """
    能量潮（OBV），无窗口，天然累积
    
    Usage:
        df.with_columns(OBV_expr().alias("obv"))
    """
    close = pl.col("close")
    volume = pl.col("volume")
    obv_delta = (
        pl.when(close > close.shift(1)).then(volume)
        .when(close < close.shift(1)).then(-volume)
        .otherwise(0)
    )
    return obv_delta.cumsum()

def OBV_df(df: pl.DataFrame, output_name: str = "obv") -> pl.DataFrame:
    """
    在 DataFrame 上添加 OBV 列
    
    Usage:
        df = OBV_df(df, output_name="obv")
    """
    return df.with_columns(OBV_expr().alias(output_name))


# ---------------- DMI ----------------
def DMI_expr(n: int = 14, m: int = 6, output_name: str = '', skip_nan: bool = True) -> pl.Expr:
    """
    动向指标（DMI/ADX）
    
    Args:
        n: TR 和方向移动平均窗口
        m: ADX 平滑窗口
        skip_nan: 是否跳过空值
        
    Returns:
        struct{atr, pdi, mdi, adx, adxr}
        
    Usage:
        df = df.with_columns(DMI_expr(14, 6).alias("dmi")).unnest("dmi")
    """
    hl = pl.col("high") - pl.col("low")
    hc = (pl.col("high") - pl.col("close").shift(1)).abs()
    lc = (pl.col("low") - pl.col("close").shift(1)).abs()
    tr = pl.max_horizontal([hl, hc, lc])
    min_samples_n = 1 if skip_nan else n
    min_samples_m = 1 if skip_nan else m
    atr = tr.rolling_mean(window_size=n, min_samples=min_samples_n)
    
    pre_high = pl.col("high").shift(1)
    pre_low = pl.col("low").shift(1)
    hd = pl.col("high") - pre_high
    ld = pre_low - pl.col("low")
    admp = pl.when((hd > 0) & (hd > ld)).then(hd).otherwise(0.0)
    admm = pl.when((ld > 0) & (ld > hd)).then(ld).otherwise(0.0)
    
    ma_admp = admp.rolling_mean(window_size=n, min_samples=min_samples_n)
    ma_admm = admm.rolling_mean(window_size=n, min_samples=min_samples_n)
    
    pdi = pl.when(atr > 0).then(ma_admp / atr * 100).otherwise(None).forward_fill()
    mdi = pl.when(atr > 0).then(ma_admm / atr * 100).otherwise(None).forward_fill()
    
    ad = (pdi - mdi).abs() / (pdi + mdi) * 100
    adx = ad.rolling_mean(window_size=m, min_samples=min_samples_m)
    adxr = (adx + adx.shift(m)) / 2
    
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{
        f"{prefix}atr": atr,
        f"{prefix}pdi": pdi,
        f"{prefix}mdi": mdi,
        f"{prefix}adx": adx,
        f"{prefix}adxr": adxr
    })

def DMI_df(df: pl.DataFrame, n: int = 14, m: int = 6, output_name: str = '', skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 DMI 五列
    
    Usage:
        df = DMI_df(df, n=14, m=6, output_name="dmi")
        # 生成列: dmi_atr, dmi_pdi, dmi_mdi, dmi_adx, dmi_adxr
    """
    return df.with_columns(DMI_expr(n, m, output_name, skip_nan).alias("dmi")).unnest("dmi")


# ---------------- SAR ----------------
def SAR_df(
    df: pl.DataFrame,
    step: float = 0.02,
    max_step: float = 0.2,
    output_name: str = "sar"
) -> pl.DataFrame:
    """
    抛物线转向指标（SAR），无法向量化，使用循环实现
    
    Usage:
        df = SAR_df(df, step=0.02, max_step=0.2, output_name="sar")
    """
    def calculate_sar(rows):
        if not rows:
            return []
        sar_vals = []
        high0, low0 = rows[0][0], rows[0][1]
        ep = high0
        af = step
        trend = 1
        sar = low0
        sar_vals.append(sar)
        for i in range(1, len(rows)):
            high, low = rows[i]
            prev_sar = sar_vals[-1]
            if trend == 1:
                sar = prev_sar + af * (ep - prev_sar)
                if low < sar:
                    trend = -1
                    sar = ep
                    af = step
                    ep = low
                else:
                    if high > ep:
                        ep = high
                        af = min(af + step, max_step)
            else:
                sar = prev_sar - af * (prev_sar - ep)
                if high > sar:
                    trend = 1
                    sar = ep
                    af = step
                    ep = high
                else:
                    if low < ep:
                        ep = low
                        af = min(af + step, max_step)
            sar_vals.append(sar)
        return sar_vals
    
    hl_rows = df.select(["high", "low"]).rows()
    sar_series = calculate_sar(hl_rows)
    return df.with_columns(pl.Series(output_name, sar_series))


# ---------------- REF / HHV / LLV ----------------
def REF_expr(column: str | pl.Expr, n: int) -> pl.Expr:
    """
    引用 N 日前的值
    
    Usage:
        df.with_columns(REF_expr("close", 1).alias("prev_close"))
    """
    return _expr(column).shift(n)

def REF_df(df: pl.DataFrame, column: str, n: int, output_name: str = 'ref') -> pl.DataFrame:
    """
    在 DataFrame 上添加引用列
    
    Usage:
        df = REF_df(df, "close", 1, "prev_close")
    """
    return df.with_columns(REF_expr(column, n).alias(output_name))

def HHV_expr(column: str | pl.Expr, n: int, skip_nan: bool = True) -> pl.Expr:
    """
    N 日最高价
    
    Usage:
        df.with_columns(HHV_expr("high", 10).alias("hhv10"))
    """
    expr = _expr(column)
    min_samples = 1 if skip_nan else n
    return expr.rolling_max(window_size=n, min_samples=min_samples)

def HHV_df(df: pl.DataFrame, column: str, n: int, output_name: str = 'hhv', skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 HHV 列
    
    Usage:
        df = HHV_df(df, "high", 10, "hhv10")
    """
    return df.with_columns(HHV_expr(column, n, skip_nan).alias(output_name))

def LLV_expr(column: str | pl.Expr, n: int, skip_nan: bool = True) -> pl.Expr:
    """
    N 日最低价
    
    Usage:
        df.with_columns(LLV_expr("low", 10).alias("llv10"))
    """
    expr = _expr(column)
    min_samples = 1 if skip_nan else n
    return expr.rolling_min(window_size=n, min_samples=min_samples)

def LLV_df(df: pl.DataFrame, column: str, n: int, output_name: str = 'llv', skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加 LLV 列
    
    Usage:
        df = LLV_df(df, "low", 10, "llv10")
    """
    return df.with_columns(LLV_expr(column, n, skip_nan).alias(output_name))

MAX_expr = HHV_expr
MIN_expr = LLV_expr
MAX_df = HHV_df
MIN_df = LLV_df


# ---------------- CROSSUP / CROSSDOWN ----------------
def CROSSUP_expr(column_a: str | pl.Expr, column_b: str | pl.Expr) -> pl.Expr:
    """
    A 上穿 B（今日 A>B 且昨日 A<=B）
    
    Usage:
        df.with_columns(CROSSUP_expr("ma5", "ma10").alias("golden_cross"))
    """
    a = _expr(column_a)
    b = _expr(column_b)
    return (a > b) & (a.shift(1) <= b.shift(1))

def CROSSUP_df(df: pl.DataFrame, column_a: str, column_b: str, output_name: str = "crossup") -> pl.DataFrame:
    """
    在 DataFrame 上添加上穿信号列
    
    Usage:
        df = CROSSUP_df(df, "ma5", "ma10", "gold_cross")
    """
    return df.with_columns(CROSSUP_expr(pl.col(column_a), pl.col(column_b)).alias(output_name))

def CROSSDOWN_expr(column_a: str | pl.Expr, column_b: str | pl.Expr) -> pl.Expr:
    """
    A 下穿 B（今日 A<B 且昨日 A>=B）
    
    Usage:
        df.with_columns(CROSSDOWN_expr("ma5", "ma10").alias("death_cross"))
    """
    a = _expr(column_a)
    b = _expr(column_b)
    return (a < b) & (a.shift(1) >= b.shift(1))

def CROSSDOWN_df(df: pl.DataFrame, column_a: str, column_b: str, output_name: str = "crossdown") -> pl.DataFrame:
    """
    在 DataFrame 上添加下穿信号列
    
    Usage:
        df = CROSSDOWN_df(df, "ma5", "ma10", "death_cross")
    """
    return df.with_columns(CROSSDOWN_expr(pl.col(column_a), pl.col(column_b)).alias(output_name))


# ---------------- BARSLAST / VALUEWHEN ----------------
def BARSLAST_expr(condition: pl.Expr) -> pl.Expr:
    """
    上次条件成立至今的周期数
    
    Usage:
        df.with_columns(BARSLAST_expr(pl.col("buy_signal")).alias("bars_since_buy"))
    """
    group = condition.cast(pl.Int32).cum_sum().fill_null(0)
    row_num = pl.int_range(0, pl.len()).over(group)
    return pl.when(condition).then(0).otherwise(row_num - 1)

def BARSLAST_df(df: pl.DataFrame, condition_col: str, output_name: str = "barslast") -> pl.DataFrame:
    """
    在 DataFrame 上添加 BARSLAST 列
    
    Usage:
        df = BARSLAST_df(df, "buy_signal", "bars_since_buy")
    """
    return df.with_columns(BARSLAST_expr(pl.col(condition_col)).alias(output_name))

def VALUEWHEN_expr(condition: pl.Expr, source: pl.Expr) -> pl.Expr:
    """
    当条件成立时取源值，否则保持上一次成立时的值
    
    Usage:
        df.with_columns(VALUEWHEN_expr(pl.col("buy_signal"), pl.col("close")).alias("buy_price"))
    """
    return pl.when(condition).then(source).otherwise(None).forward_fill()

def VALUEWHEN_df(df: pl.DataFrame, condition_col: str, source_col: str, output_name: str = "valuewhen") -> pl.DataFrame:
    """
    在 DataFrame 上添加 VALUEWHEN 列
    
    Usage:
        df = VALUEWHEN_df(df, "buy_signal", "close", "buy_price")
    """
    return df.with_columns(VALUEWHEN_expr(pl.col(condition_col), pl.col(source_col)).alias(output_name))


# ---------------- STD ----------------
def STD_expr(column: str | pl.Expr, window: int, skip_nan: bool = True) -> pl.Expr:
    """
    滚动标准差
    
    Usage:
        df.with_columns(STD_expr("close", 20).alias("volatility"))
    """
    col = _expr(column)
    min_samples = 1 if skip_nan else window
    return col.rolling_std(window_size=window, ddof=0, min_samples=min_samples)

def STD_df(df: pl.DataFrame, column: str, window: int, output_name: str = "std", skip_nan: bool = True) -> pl.DataFrame:
    """
    在 DataFrame 上添加标准差列
    
    Usage:
        df = STD_df(df, "close", 20, "vol20")
    """
    return df.with_columns(STD_expr(column, window, skip_nan).alias(output_name))


# ---------------- DMA ----------------
def DMA_expr(
    column: str | pl.Expr = "close",
    short: int = 10,
    long: int = 50,
    skip_nan: bool = True
) -> pl.Expr:
    """
    差异均线（DMA）= 短期 MA - 长期 MA
    
    Usage:
        df.with_columns(DMA_expr("close", 10, 50).alias("dma"))
    """
    col = _expr(column)
    ma_short = col.rolling_mean(window_size=short, min_samples=1 if skip_nan else short)
    ma_long = col.rolling_mean(window_size=long, min_samples=1 if skip_nan else long)
    return ma_short - ma_long

def DMA_df(
    df: pl.DataFrame,
    column: str = "close",
    short: int = 10,
    long: int = 50,
    output_name: str = "dma",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 DMA 列
    
    Usage:
        df = DMA_df(df, short=10, long=50, output_name="dma")
    """
    return df.with_columns(DMA_expr(column, short, long, skip_nan).alias(output_name))


# ---------------- MTM ----------------
def MTM_expr(column: str | pl.Expr, n: int) -> pl.Expr:
    """
    动量指标（MTM）= 当前价 - N 日前价格
    
    Usage:
        df.with_columns(MTM_expr("close", 10).alias("mtm10"))
    """
    col = _expr(column)
    return col - col.shift(n)

def MTM_df(df: pl.DataFrame, column: str, n: int, output_name: str = "mtm") -> pl.DataFrame:
    """
    在 DataFrame 上添加 MTM 列
    
    Usage:
        df = MTM_df(df, "close", 10, "mtm10")
    """
    return df.with_columns(MTM_expr(column, n).alias(output_name))


# ---------------- Donchian Channel ----------------
def DONCHIAN_expr(
    high: pl.Expr | str = "high",
    low: pl.Expr | str = "low",
    window: int = 20,
    output_name: str = "donchian_",
    skip_nan: bool = True
) -> pl.Expr:
    """
    唐奇安通道
    
    Returns:
        struct{upper, lower, middle}
        
    Usage:
        df = df.with_columns(DONCHIAN_expr("high", "low", 20).alias("_donchian")).unnest("_donchian")
    """
    high = _expr(high)
    low = _expr(low)
    min_samples = 1 if skip_nan else window
    upper = high.rolling_max(window_size=window, min_samples=min_samples)
    lower = low.rolling_min(window_size=window, min_samples=min_samples)
    middle = (upper + lower) / 2.0
    return pl.struct(**{
        f"{output_name}upper": upper,
        f"{output_name}lower": lower,
        f"{output_name}middle": middle
    })

def DONCHIAN_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    window: int = 20,
    output_name: str = "donchian_",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加唐奇安通道三列
    
    Usage:
        df = DONCHIAN_df(df, window=20, output_name="dc")
        # 生成列: dc_upper, dc_lower, dc_middle
    """
    return (
        df
        .with_columns(DONCHIAN_expr(high_col, low_col, window, output_name, skip_nan).alias("_donchian"))
        .unnest("_donchian")
    )


# ---------------- ROC ----------------
def ROC_expr(column: str | pl.Expr, n: int) -> pl.Expr:
    """
    变动率指标（ROC）= (当前价 - N 日前价) / N 日前价 * 100
    
    Usage:
        df.with_columns(ROC_expr("close", 12).alias("roc12"))
    """
    col = _expr(column)
    return (col - col.shift(n)) / col.shift(n) * 100

def ROC_df(df: pl.DataFrame, column: str, n: int, output_name: str = "roc") -> pl.DataFrame:
    """
    在 DataFrame 上添加 ROC 列
    
    Usage:
        df = ROC_df(df, "close", 12, "roc12")
    """
    return df.with_columns(ROC_expr(column, n).alias(output_name))


# ---------------- WR ----------------
def WR_expr(
    high: str | pl.Expr = "high",
    low: str | pl.Expr = "low",
    close: str | pl.Expr = "close",
    n: int = 14,
    skip_nan: bool = True
) -> pl.Expr:
    """
    威廉指标（WR）= -(HH - CLOSE) / (HH - LL) * 100
    
    Usage:
        df.with_columns(WR_expr("high", "low", "close", 14).alias("wr14"))
    """
    high, low, close = map(_expr, [high, low, close])
    min_samples = 1 if skip_nan else n
    hh = high.rolling_max(window_size=n, min_samples=min_samples)
    ll = low.rolling_min(window_size=n, min_samples=min_samples)
    return (hh - close) / (hh - ll) * -100

def WR_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    n: int = 14,
    output_name: str = "wr",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 WR 列
    
    Usage:
        df = WR_df(df, n=14, output_name="wr14")
    """
    return df.with_columns(WR_expr(high_col, low_col, close_col, n, skip_nan).alias(output_name))


# ---------------- BIAS ----------------
def BIAS_expr(close: str | pl.Expr, ma_window: int, skip_nan: bool = True) -> pl.Expr:
    """
    乖离率（BIAS）= (收盘价 - MA) / MA * 100
    
    Usage:
        df.with_columns(BIAS_expr("close", 12).alias("bias12"))
    """
    close = _expr(close)
    ma = close.rolling_mean(window_size=ma_window, min_samples=1 if skip_nan else ma_window)
    return (close - ma) / ma * 100

def BIAS_df(
    df: pl.DataFrame,
    close_col: str = "close",
    ma_window: int = 12,
    output_name: str = "bias",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 BIAS 列
    
    Usage:
        df = BIAS_df(df, ma_window=12, output_name="bias12")
    """
    return df.with_columns(BIAS_expr(close_col, ma_window, skip_nan).alias(output_name))


# ---------------- ENV (Envelope) ----------------
def ENV_expr(
    close: str | pl.Expr = "close",
    window: int = 20,
    percent: float = 2.0,
    output_name: str = "env_",
    skip_nan: bool = True
) -> pl.Expr:
    """
    价格通道（ENV）= MA ± MA * percent%
    
    Returns:
        struct{upper, lower, mid}
        
    Usage:
        df = df.with_columns(ENV_expr("close", 20, 2.0).alias("_env")).unnest("_env")
    """
    close = _expr(close)
    min_samples = 1 if skip_nan else window
    ma = close.rolling_mean(window_size=window, min_samples=min_samples)
    dev = ma * (percent / 100.0)
    return pl.struct(**{
        f"{output_name}upper": ma + dev,
        f"{output_name}lower": ma - dev,
        f"{output_name}mid": ma
    })

def ENV_df(
    df: pl.DataFrame,
    close_col: str = "close",
    window: int = 20,
    percent: float = 2.0,
    output_name: str = "env_",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加价格通道三列
    
    Usage:
        df = ENV_df(df, window=20, percent=2.0, output_name="env")
        # 生成列: env_upper, env_lower, env_mid
    """
    return df.with_columns(ENV_expr(close_col, window, percent, output_name, skip_nan).alias("_env")).unnest("_env")


# ---------------- VOL_MA ----------------
def VOL_MA_expr(volume: str | pl.Expr, window: int, skip_nan: bool = True) -> pl.Expr:
    """
    成交量移动平均
    
    Usage:
        df.with_columns(VOL_MA_expr("volume", 5).alias("vol_ma5"))
    """
    vol = _expr(volume)
    min_samples = 1 if skip_nan else window
    return vol.rolling_mean(window_size=window, min_samples=min_samples)

def VOL_MA_df(
    df: pl.DataFrame,
    volume_col: str = "volume",
    window: int = 5,
    output_name: str = "vol_ma",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加成交量 MA 列
    
    Usage:
        df = VOL_MA_df(df, window=5, output_name="vol_ma5")
    """
    return df.with_columns(VOL_MA_expr(volume_col, window, skip_nan).alias(output_name))


# ---------------- CMO ----------------
def CMO_expr(close: str | pl.Expr = "close", window: int = 14, skip_nan: bool = True) -> pl.Expr:
    """
    钱德动量摆动指标（CMO）
    
    Usage:
        df.with_columns(CMO_expr("close", 14).alias("cmo"))
    """
    price = _expr(close)
    diff = price.diff()
    up = diff.clip(0, None)
    down = (-diff).clip(0, None)
    min_samples = 1 if skip_nan else window
    sum_up = up.rolling_sum(window_size=window, min_samples=min_samples)
    sum_down = down.rolling_sum(window_size=window, min_samples=min_samples)
    return (sum_up - sum_down) / (sum_up + sum_down) * 100

def CMO_df(
    df: pl.DataFrame,
    close_col: str = "close",
    window: int = 14,
    output_name: str = "cmo",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 CMO 列
    
    Usage:
        df = CMO_df(df, window=14, output_name="cmo14")
    """
    return df.with_columns(CMO_expr(close_col, window, skip_nan).alias(output_name))


# ---------------- TRIX ----------------
def TRIX_expr(close: str | pl.Expr = "close", window: int = 12) -> pl.Expr:
    """
    三重指数平滑平均线（TRIX），基于 EMA，天然处理 NaN
    
    Usage:
        df.with_columns(TRIX_expr("close", 12).alias("trix"))
    """
    price = _expr(close)
    ema1 = price.ewm_mean(span=window, adjust=False)
    ema2 = ema1.ewm_mean(span=window, adjust=False)
    ema3 = ema2.ewm_mean(span=window, adjust=False)
    trix = (ema3 - ema3.shift(1)) / ema3.shift(1) * 100
    return trix

def TRIX_df(
    df: pl.DataFrame,
    close_col: str = "close",
    window: int = 12,
    output_name: str = "trix"
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 TRIX 列
    
    Usage:
        df = TRIX_df(df, window=12, output_name="trix12")
    """
    return df.with_columns(TRIX_expr(close_col, window).alias(output_name))


# ---------------- VR ----------------
def VR_expr(
    close: str | pl.Expr = "close",
    volume: str | pl.Expr = "volume",
    window: int = 26,
    skip_nan: bool = True
) -> pl.Expr:
    """
    成交量比率（VR）
    
    Usage:
        df.with_columns(VR_expr("close", "volume", 26).alias("vr"))
    """
    close = _expr(close)
    volume = _expr(volume)
    is_up = close > close.shift(1)
    is_down = close < close.shift(1)
    is_equal = close == close.shift(1)
    min_samples = 1 if skip_nan else window
    av = pl.when(is_up).then(volume).otherwise(0).rolling_sum(window_size=window, min_samples=min_samples)
    bv = pl.when(is_down).then(volume).otherwise(0).rolling_sum(window_size=window, min_samples=min_samples)
    cv = pl.when(is_equal).then(volume).otherwise(0).rolling_sum(window_size=window, min_samples=min_samples)
    denominator = pl.when(bv + cv/2 == 0).then(1).otherwise(bv + cv/2)
    vr = (av + cv/2) / denominator * 100
    return vr

def VR_df(
    df: pl.DataFrame,
    close_col: str = "close",
    volume_col: str = "volume",
    window: int = 26,
    output_name: str = "vr",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 VR 列
    
    Usage:
        df = VR_df(df, window=26, output_name="vr26")
    """
    return df.with_columns(VR_expr(close_col, volume_col, window, skip_nan).alias(output_name))


# ---------------- VWAP ----------------
def VWAP_expr(
    high: str | pl.Expr = "high",
    low: str | pl.Expr = "low",
    close: str | pl.Expr = "close",
    volume: str | pl.Expr = "volume"
) -> pl.Expr:
    """
    成交量加权平均价（VWAP），全周期累积
    
    Usage:
        df.with_columns(VWAP_expr("high", "low", "close", "volume").alias("vwap"))
    """
    high, low, close, volume = map(_expr, [high, low, close, volume])
    typical_price = (high + low + close) / 3.0
    return (typical_price * volume).cumsum() / volume.cumsum()

def VWAP_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
    output_name: str = "vwap"
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 VWAP 列（全周期）
    
    Usage:
        df = VWAP_df(df, output_name="vwap")
    """
    return df.with_columns(VWAP_expr(high_col, low_col, close_col, volume_col).alias(output_name))

def VWAP_daily_df(
    df: pl.DataFrame,
    date_col: str,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
    output_name: str = "vwap"
) -> pl.DataFrame:
    """
    按日期分组计算日 VWAP
    
    Usage:
        df = VWAP_daily_df(df, date_col="date", output_name="vwap_daily")
    """
    return df.with_columns(
        VWAP_expr(high_col, low_col, close_col, volume_col)
        .over(pl.col(date_col))
        .alias(output_name)
    )


# ---------------- MFI ----------------
def MFI_expr(
    high: str | pl.Expr = "high",
    low: str | pl.Expr = "low",
    close: str | pl.Expr = "close",
    volume: str | pl.Expr = "volume",
    window: int = 14,
    skip_nan: bool = True
) -> pl.Expr:
    """
    资金流量指标（MFI）
    
    Usage:
        df.with_columns(MFI_expr("high", "low", "close", "volume", 14).alias("mfi"))
    """
    high, low, close, volume = map(_expr, [high, low, close, volume])
    typical_price = (high + low + close) / 3.0
    raw_money_flow = typical_price * volume
    delta = typical_price.diff()
    positive_flow = pl.when(delta > 0).then(raw_money_flow).otherwise(0.0)
    negative_flow = pl.when(delta < 0).then(raw_money_flow).otherwise(0.0)
    min_samples = 1 if skip_nan else window
    pos_sum = positive_flow.rolling_sum(window_size=window, min_samples=min_samples)
    neg_sum = negative_flow.rolling_sum(window_size=window, min_samples=min_samples)
    mfi = 100.0 - (100.0 / (1.0 + pos_sum / neg_sum))
    return mfi

def MFI_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    volume_col: str = "volume",
    window: int = 14,
    output_name: str = "mfi",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 MFI 列
    
    Usage:
        df = MFI_df(df, window=14, output_name="mfi14")
    """
    return df.with_columns(MFI_expr(high_col, low_col, close_col, volume_col, window, skip_nan).alias(output_name))


# ---------------- STOCH_RSI ----------------
def STOCH_RSI_expr(
    close: str | pl.Expr = "close",
    rsi_window: int = 14,
    stoch_window: int = 3,
    smooth_k: int = 3,
    smooth_d: int = 3,
    output_name: str = "stoch_rsi_",
    skip_nan: bool = True
) -> pl.Expr:
    """
    随机 RSI（Stochastic RSI）
    
    Returns:
        struct{k, d}
        
    Usage:
        df = df.with_columns(STOCH_RSI_expr("close", 14, 3, 3, 3).alias("_stoch_rsi")).unnest("_stoch_rsi")
    """
    close = _expr(close)
    delta = close.diff()
    up = delta.clip(0, None)
    down = (-delta).clip(0, None)
    ema_up = up.ewm_mean(span=rsi_window, adjust=False)
    ema_down = down.ewm_mean(span=rsi_window, adjust=False)
    rs = ema_up / ema_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    
    min_samples_stoch = 1 if skip_nan else stoch_window
    min_samples_smooth = 1 if skip_nan else smooth_k
    
    rsi_min = rsi.rolling_min(window_size=stoch_window, min_samples=min_samples_stoch)
    rsi_max = rsi.rolling_max(window_size=stoch_window, min_samples=min_samples_stoch)
    stoch_k = pl.when(rsi_max == rsi_min).then(0.0).otherwise((rsi - rsi_min) / (rsi_max - rsi_min) * 100)
    stoch_k_smooth = stoch_k.rolling_mean(window_size=smooth_k, min_samples=min_samples_smooth)
    stoch_d = stoch_k_smooth.rolling_mean(window_size=smooth_d, min_samples=min_samples_smooth)
    
    return pl.struct(**{
        f"{output_name}k": stoch_k_smooth,
        f"{output_name}d": stoch_d
    })

def STOCH_RSI_df(
    df: pl.DataFrame,
    close_col: str = "close",
    rsi_window: int = 14,
    stoch_window: int = 3,
    smooth_k: int = 3,
    smooth_d: int = 3,
    output_name: str = "stoch_rsi",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 Stochastic RSI 两列
    
    Usage:
        df = STOCH_RSI_df(df, rsi_window=14, stoch_window=3, smooth_k=3, smooth_d=3, output_name="srsi")
        # 生成列: srsi_k, srsi_d
    """
    return df.with_columns(
        STOCH_RSI_expr(close_col, rsi_window, stoch_window, smooth_k, smooth_d, output_name, skip_nan).alias("_stoch_rsi")
    ).unnest("_stoch_rsi")


# ---------------- SUPERTREND ----------------
def SUPERTREND_expr(
    high: str | pl.Expr = "high",
    low: str | pl.Expr = "low",
    close: str | pl.Expr = "close",
    atr_window: int = 10,
    multiplier: float = 3.0,
    output_name: str = "",
    skip_nan: bool = True
) -> pl.Expr:
    """
    超级趋势指标（Supertrend）
    
    Returns:
        struct{trend, direction}
        
    Usage:
        df = df.with_columns(SUPERTREND_expr("high", "low", "close", 10, 3.0).alias("st")).unnest("st")
    """
    high, low, close = map(_expr, [high, low, close])
    hl = high - low
    hc = (high - close.shift(1)).abs()
    lc = (low - close.shift(1)).abs()
    tr = pl.max_horizontal([hl, hc, lc])
    min_samples = 1 if skip_nan else atr_window
    atr = tr.rolling_mean(window_size=atr_window, min_samples=min_samples)
    src = (high + low) / 2.0
    upper_band = src + multiplier * atr
    lower_band = src - multiplier * atr
    
    # 初始化
    trend = pl.lit(1)
    direction = pl.lit(1)
    
    # 迭代逻辑需用循环，但 Polars 不支持。此处提供近似向量化版本（可能不完全准确）
    # 实际建议用 TA-Lib 或自定义 UDF
    is_uptrend = close > lower_band.shift(1)
    is_downtrend = close < upper_band.shift(1)
    
    # 简化版：仅返回上下轨（常用于策略）
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{
        f"{prefix}upper": upper_band,
        f"{prefix}lower": lower_band
    })

def SUPERTREND_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    atr_window: int = 10,
    multiplier: float = 3.0,
    output_name: str = "supertrend",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    在 DataFrame 上添加 Supertrend 上下轨
    
    Usage:
        df = SUPERTREND_df(df, atr_window=10, multiplier=3.0, output_name="st")
        # 生成列: st_upper, st_lower
    """
    return df.with_columns(
        SUPERTREND_expr(high_col, low_col, close_col, atr_window, multiplier, output_name, skip_nan).alias("_st")
    ).unnest("_st")

def COUNT_expr(cond: pl.Expr, n: int) -> pl.Expr:
    """
    统计 n 周期内 cond 为 True 的次数。
    若 n == 0，则从序列开头累计。
    df = df.with_columns(
                        (pl.col("close") > pl.col("open")).alias("is_bull")
                    ).with_columns(
                        COUNT_expr(pl.col("is_bull"), 5).alias("bull_count_5")
                    )
    """
    cond_int = cond.cast(pl.Int32)
    if n == 0:
        return cond_int.cum_sum()
    else:
        return cond_int.rolling_sum(window_size=n, min_samples=1)
def COUNT_df(df: pl.DataFrame, cond_col: str, n: int, output_col: str = "count") -> pl.DataFrame:
    return df.with_columns(COUNT_expr(pl.col(cond_col), n).alias(output_col))

def TRMA_expr(series: Union[str, pl.Expr], n: int, skip_nan: bool = True) -> pl.Expr:
    series = _expr(series)
    if n <= 0:
        return pl.lit(None).cast(pl.Float64)
    
    min_samples = 1 if skip_nan else n
    
    if n % 2 == 0:
        n1, n2 = n // 2, n // 2 + 1
    else:
        n1 = n2 = (n + 1) // 2
    
    ma1 = series.rolling_mean(window_size=n1, min_samples=min_samples)
    return ma1.rolling_mean(window_size=n2, min_samples=min_samples)

def TRMA_df(df: pl.DataFrame, col: str, n: int, output_col: str = "trma") -> pl.DataFrame:
    '''
    df = TRMA_df(df, "close", 10, "trma_10")
    '''
    return df.with_columns(TRMA_expr(col, n).alias(output_col))

def harmean_expr(series: Union[str, pl.Expr], n: int, skip_nan: bool = True) -> pl.Expr:
    '''
    注意：若窗口内有 0 或负值，1/x 会出错或无意义。建议只用于正数序列（如价格）
    '''
    series = _expr(series)
    if n <= 0:
        return pl.lit(None).cast(pl.Float64)
    
    min_samples = 1 if skip_nan else n
    inv_sum = (1.0 / series).rolling_sum(window_size=n, min_samples=min_samples)
    count_valid = series.is_not_null().cast(pl.Int32).rolling_sum(window_size=n, min_samples=1)
    
    effective_n = pl.min_horizontal([count_valid, pl.lit(n)])
    result = pl.when(inv_sum == 0).then(None).otherwise(effective_n / inv_sum)
    
    if not skip_nan:
        result = pl.when(count_valid < n).then(None).otherwise(result)
    return result

def harmean_df(df: pl.DataFrame, col: str, n: int, output_col: str = "harmean") -> pl.DataFrame:
    return df.with_columns(harmean_expr(col, n).alias(output_col))

def numpow_expr(series: Union[str, pl.Expr], n: int, m: float) -> pl.Expr:
    '''
    自然数幂方和
    '''
    series = _expr(series)
    if n <= 0:
        return pl.lit(None).cast(pl.Float64)
    
    terms = [
        ((n - i) ** m) * series.shift(i)
        for i in range(n)
    ]
    return pl.sum_horizontal(terms)

def numpow_df(df: pl.DataFrame, col: str, n: int, m: float, output_col: str = "numpow") -> pl.DataFrame:
    '''
    df = numpow_df(df, "close", 5, 2, "weighted_close")
    '''
    return df.with_columns(numpow_expr(col, n, m).alias(output_col))


def ABS_expr(series: Union[str, pl.Expr]) -> pl.Expr:
    return _expr(series).abs()

def ABS_df(df: pl.DataFrame, col: str, output_col: str = "abs") -> pl.DataFrame:
    '''
    实际上可直接用 pl.col("x").abs()，但为统一接口保留函数
    '''
    return df.with_columns(ABS_expr(col).alias(output_col))

def ICHIMOKU_expr(
    high: str | pl.Expr = "high",
    low: str | pl.Expr = "low",
    close: str | pl.Expr = "close",
    tenkan_period: int = 9,
    kijun_period: int = 26,
    senkou_span_b_period: int = 52,
    output_name: str = "",
    skip_nan: bool = True
) -> pl.Expr:
    """
    一目均衡表（Ichimoku Cloud）
    
    Returns:
        struct{tenkan, kijun, senkou_a, senkou_b, chikou}
        
    Usage:
        df = df.with_columns(ICHIMOKU_expr("high", "low", "close", 9, 26, 52).alias("ichimoku")).unnest("ichimoku")
    """
    high, low, close = map(_expr, [high, low, close])
    
    def rolling_min_max(col_high, col_low, window, skip):
        ms = 1 if skip else window
        return (
            col_high.rolling_max(window_size=window, min_samples=ms),
            col_low.rolling_min(window_size=window, min_samples=ms)
        )
    
    tenkan_h, tenkan_l = rolling_min_max(high, low, tenkan_period, skip_nan)
    tenkan = (tenkan_h + tenkan_l) / 2
    
    kijun_h, kijun_l = rolling_min_max(high, low, kijun_period, skip_nan)
    kijun = (kijun_h + kijun_l) / 2
    
    senkou_b_h, senkou_b_l = rolling_min_max(high, low, senkou_span_b_period, skip_nan)
    senkou_b = (senkou_b_h + senkou_b_l) / 2
    
    senkou_a = (tenkan + kijun) / 2
    
    # 先行带需向前偏移（Senkou Span 向前推 kijun_period 根 K 线）
    # Polars 无法直接“未来填充”，故返回原始值，由用户自行 shift(-kijun_period)
    chikou = close.shift(-kijun_period)  # 滞后线：当前收盘价向后推 kijun_period
    
    prefix = output_name + "_" if output_name else ""
    return pl.struct(**{
        f"{prefix}tenkan": tenkan,
        f"{prefix}kijun": kijun,
        f"{prefix}senkou_a_raw": senkou_a,      # 用户可手动 .shift(-kijun_period)
        f"{prefix}senkou_b_raw": senkou_b,
        f"{prefix}chikou": chikou
    })

def ICHIMOKU_df(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    close_col: str = "close",
    tenkan_period: int = 9,
    kijun_period: int = 26,
    senkou_span_b_period: int = 52,
    output_name: str = "ichimoku",
    skip_nan: bool = True
) -> pl.DataFrame:
    """
    添加 Ichimoku 五列（注意：Senkou 需手动前移）
    
    Usage:
        df = ICHIMOKU_df(df, output_name="ichi")
        # 再手动处理先行带：
        df = df.with_columns([
            pl.col("ichi_senkou_a_raw").shift(-26).alias("ichi_senkou_a"),
            pl.col("ichi_senkou_b_raw").shift(-26).alias("ichi_senkou_b")
        ])
    """
    return df.with_columns(
        ICHIMOKU_expr(high_col, low_col, close_col, tenkan_period, kijun_period, senkou_span_b_period, output_name, skip_nan).alias("ichimoku")
    ).unnest("ichimoku")

def _linear_regression_channel_numpy(
    prices: np.ndarray,
    window: int,
    k: float = 2.0,
    ddof: int = 0  # 0=总体标准差, 1=样本标准差
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    使用 NumPy 向量化计算滚动线性回归通道
    
    Returns:
        mid, upper, lower (all np.ndarray, same length as prices)
    """
    n = len(prices)
    if n < window or window <= 0:
        nan_array = np.full(n, np.nan)
        return nan_array, nan_array, nan_array

    if window == 1:
        # 特殊处理：窗口为1时，回归线就是价格本身，std=0
        mid = prices.astype(np.float64)
        upper = mid + k * 0.0
        lower = mid - k * 0.0
        return mid, upper, lower

    # 构造滚动窗口
    windows = np.lib.stride_tricks.sliding_window_view(prices, window_shape=window)

    x = np.arange(window, dtype=np.float64)
    x_mean = (window - 1) / 2.0
    S_xx = np.sum((x - x_mean) ** 2)
    
    if S_xx == 0:
        # 理论上不会发生（window>=2 时 S_xx > 0），但防御性编程
        nan_array = np.full(n, np.nan)
        return nan_array, nan_array, nan_array

    y_means = np.mean(windows, axis=1)
    xy_sum = np.sum(x * windows, axis=1)
    S_xy = xy_sum - window * x_mean * y_means

    slopes = S_xy / S_xx
    intercepts = y_means - slopes * x_mean
    y_pred = slopes * (window - 1) + intercepts

    # 拟合值与残差
    y_fitted = slopes[:, None] * x[None, :] + intercepts[:, None]
    residuals = windows - y_fitted
    std_devs = np.std(residuals, axis=1, ddof=ddof)

    full_length = n
    mid = np.full(full_length, np.nan)
    upper = np.full(full_length, np.nan)
    lower = np.full(full_length, np.nan)

    start_idx = window - 1
    mid[start_idx:] = y_pred
    upper[start_idx:] = y_pred + k * std_devs
    lower[start_idx:] = y_pred - k * std_devs

    return mid, upper, lower


def LINREG_CHANNEL_df(
    df: pl.DataFrame,
    price_col: str = "close",
    window: int = 14,
    k: float = 2.0,
    output_prefix: str = "lr",
    ddof: int = 0
) -> pl.DataFrame:
    """
    高性能线性回归通道（基于 NumPy 向量化）
    k=1.5～2.0 较常用
    df = LINREG_CHANNEL_df(df, price_col="close", window=20, k=2.0, output_prefix="lr20")
    """
    prices = df[price_col].to_numpy()
    mid, upper, lower = _linear_regression_channel_numpy(prices, window, k, ddof)

    return df.with_columns([
        pl.Series(f"{output_prefix}_mid", mid, dtype=pl.Float64),
        pl.Series(f"{output_prefix}_upper", upper, dtype=pl.Float64),
        pl.Series(f"{output_prefix}_lower", lower, dtype=pl.Float64)
    ])


# =============================================================================
# MACD 背离检测（增强版）
# =============================================================================

def MACD_divergence_expr(
    close: pl.Expr | str = "close",
    macd_diff: pl.Expr | str = "diff",
    lookback: int = 20,
    min_interval: int = 5,  # 两次 pivot 至少间隔 N 根 K 线
    output_prefix: str = "macd_"
) -> pl.Expr:
    """
    检测 MACD 顶背离与底背离（增强版）
    建议 lookback=20, min_interval=8（避免噪音）
    
    新增：
      - min_interval: 避免相邻 pivot 过近导致假信号
    """
    close = _expr(close)
    macd_diff = _expr(macd_diff)
    
    is_high = (close == HHV_expr(close, lookback)) & (close > REF_expr(close, 1))
    is_low  = (close == LLV_expr(close, lookback)) & (close < REF_expr(close, 1))

    # 记录最近一次高点/低点
    last_high_price = VALUEWHEN_expr(is_high, close)
    last_high_macd  = VALUEWHEN_expr(is_high, macd_diff)
    last_high_time  = VALUEWHEN_expr(is_high, pl.int_range(0, pl.len()))

    prev_high_price = REF_expr(last_high_price, 1)
    prev_high_macd  = REF_expr(last_high_macd, 1)
    prev_high_time  = REF_expr(last_high_time, 1)

    last_low_price = VALUEWHEN_expr(is_low, close)
    last_low_macd  = VALUEWHEN_expr(is_low, macd_diff)
    last_low_time  = VALUEWHEN_expr(is_low, pl.int_range(0, pl.len()))

    prev_low_price = REF_expr(last_low_price, 1)
    prev_low_macd  = REF_expr(last_low_macd, 1)
    prev_low_time  = REF_expr(last_low_time, 1)

    # 时间间隔过滤
    high_valid_interval = (last_high_time - prev_high_time) >= min_interval
    low_valid_interval  = (last_low_time - prev_low_time) >= min_interval

    bearish = (
        is_high &
        prev_high_price.is_not_null() &
        high_valid_interval &
        (close > prev_high_price) &
        (macd_diff < prev_high_macd)
    )

    bullish = (
        is_low &
        prev_low_price.is_not_null() &
        low_valid_interval &
        (close < prev_low_price) &
        (macd_diff > prev_low_macd)
    )

    return pl.struct(**{
        f"{output_prefix}bearish": bearish.fill_null(False),
        f"{output_prefix}bullish": bullish.fill_null(False)
    })


def MACD_divergence_df(
    df: pl.DataFrame,
    close_col: str = "close",
    macd_diff_col: str = "diff",
    lookback: int = 20,
    min_interval: int = 5,
    output_prefix: str = "macd_"
) -> pl.DataFrame:
    """
    在 DataFrame 中添加 MACD 背离信号列
    # 假设 df 已有 "close" 和 "diff" 列
    df = MACD_divergence_df(
        df,
        close_col="close",
        macd_diff_col="diff",
        lookback=20,        # 在最近20根K线内找高低点
        min_interval=8,     # 两次高点/低点至少间隔8根K线
        output_prefix="div_"
    )

    # 查看所有背离信号
    signals = df.filter(pl.col("div_bearish") | pl.col("div_bullish"))
    print(signals.select(["date", "close", "diff", "div_bearish", "div_bullish"]))
    """
    return (
        df
        .with_columns(
            MACD_divergence_expr(
                close=close_col,
                macd_diff=macd_diff_col,
                lookback=lookback,
                min_interval=min_interval,
                output_prefix=output_prefix
            ).alias("_div_signal")
        )
        .unnest("_div_signal")
    )

def zigzag_core(
    high: np.ndarray,
    low: np.ndarray,
    threshold: float,
    mode: str = "percent"
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    n = len(high)
    if n == 0:
        return (
            np.array([]),
            np.array([], dtype=np.int32),
            np.array([]),
            np.array([], dtype='<U4')
        )
    
    max_pivots = min(n, 10000)
    indices_buf = np.empty(max_pivots, dtype=np.int32)
    prices_buf = np.empty(max_pivots, dtype=np.float64)
    types_buf = np.empty(max_pivots, dtype='<U4')
    count = 0

    zigzag_prices = np.full(n, np.nan, dtype=np.float64)
    
    last_pivot_price = (high[0] + low[0]) / 2.0
    last_pivot_idx = 0
    trend = 0

    for i in range(1, n):
        h, l = high[i], low[i]
        
        if mode == "percent":
            up_move = (h - last_pivot_price) / abs(last_pivot_price + 1e-10)
            down_move = (last_pivot_price - l) / abs(last_pivot_price + 1e-10)
        else:  # points
            up_move = h - last_pivot_price
            down_move = last_pivot_price - l

        if trend <= 0 and up_move >= threshold:
            if trend == -1 and count < max_pivots:
                zigzag_prices[last_pivot_idx] = last_pivot_price
                indices_buf[count] = last_pivot_idx
                prices_buf[count] = last_pivot_price
                types_buf[count] = 'low'
                count += 1
            last_pivot_price = h
            last_pivot_idx = i
            trend = 1

        elif trend >= 0 and down_move >= threshold:
            if trend == 1 and count < max_pivots:
                zigzag_prices[last_pivot_idx] = last_pivot_price
                indices_buf[count] = last_pivot_idx
                prices_buf[count] = last_pivot_price
                types_buf[count] = 'high'
                count += 1
            last_pivot_price = l
            last_pivot_idx = i
            trend = -1

    return (
        zigzag_prices,
        indices_buf[:count],
        prices_buf[:count],
        types_buf[:count]
    )


def ZigZag(
    df: pl.DataFrame,
    high_col: str = "high",
    low_col: str = "low",
    threshold: float = 0.05,
    mode: str = "percent",
    pivot_col: str = "zigzag_pivot",
    price_col: str = "zigzag_price",
    return_pivots: bool = False
) -> pl.DataFrame | Tuple[pl.DataFrame, pl.DataFrame]:
    """
    在 DataFrame 中添加 ZigZag 转折点标记。
    threshold=0.03（3%）适合日线，1分钟线可用 0.005
    # 假设 df 有 "high", "low", "close"
    df_zz, pivot_table = ZigZag(
        df,
        high_col="high",
        low_col="low",
        threshold=0.03,      # 3% 波动才确认转折
        mode="percent",      # 也可用 "points"（绝对价格差）
        pivot_col="zz_pivot",
        price_col="zz_price",
        return_pivots=True   # 同时返回转折点子表
    )

    # 查看主表中的转折点
    print(df_zz.filter(pl.col("zz_pivot").is_not_null()).select(["date", "zz_pivot", "zz_price"]))

    # 查看精简的 pivot 子表（便于分析）
    print(pivot_table.select(["date", "close", "zigzag_type", "zigzag_price"]))
    """
    high = df[high_col].to_numpy()
    low = df[low_col].to_numpy()
    
    zz_prices, idxs, prices, types = zigzag_core(high, low, threshold, mode)
    
    # 安全构建 pivot_col：先全为 null，再填充
    pivot_series = np.full(len(df), None, dtype=object)
    if len(idxs) > 0:
        pivot_series[idxs] = types  # 自动广播

    df_with_zz = df.with_columns(
        pl.Series(pivot_col, pivot_series, dtype=pl.Utf8),
        pl.Series(price_col, zz_prices, dtype=pl.Float64)
    )
    
    if not return_pivots:
        return df_with_zz

    # 构建 pivot 子表
    if len(idxs) == 0:
        pivot_df = pl.DataFrame({
            "original_index": pl.Series([], dtype=pl.Int32),
            "zigzag_price": pl.Series([], dtype=pl.Float64),
            "zigzag_type": pl.Series([], dtype=pl.Utf8)
        })
        # 补充原始列（空）
        for col in df.columns:
            pivot_df = pivot_df.with_columns(pl.lit(None).cast(df[col].dtype).alias(col))
    else:
        pivot_df = (
            df
            .with_row_index("_idx")
            .filter(pl.col("_idx").is_in(idxs.tolist()))
            .with_columns(
                pl.Series("zigzag_price", prices),
                pl.Series("zigzag_type", types),
                pl.col("_idx").alias("original_index")
            )
            .drop("_idx")
        )

    return df_with_zz, pivot_df

def MACD_divergence_zigzag_expr(
    close: str | pl.Expr,
    macd_diff: str | pl.Expr,
    zigzag_pivot: str | pl.Expr,  # 来自 ZigZag 的列
    output_prefix: str = "zz_div_"
) -> pl.Expr:
    '''
    ZigZag + MACD 背离
    df = df.with_columns(
                        MACD_divergence_zigzag_expr("close", "diff", "zz_pivot", "zz_div_")
                        .alias("_tmp")
                    ).unnest("_tmp")
    '''
    close = _expr(close)
    macd_diff = _expr(macd_diff)
    is_high = _expr(zigzag_pivot) == "high"
    is_low  = _expr(zigzag_pivot) == "low"

    last_high_price = VALUEWHEN_expr(is_high, close)
    last_high_macd  = VALUEWHEN_expr(is_high, macd_diff)
    prev_high_price = REF_expr(last_high_price, 1)
    prev_high_macd  = REF_expr(last_high_macd, 1)

    last_low_price = VALUEWHEN_expr(is_low, close)
    last_low_macd  = VALUEWHEN_expr(is_low, macd_diff)
    prev_low_price = REF_expr(last_low_price, 1)
    prev_low_macd  = REF_expr(last_low_macd, 1)

    bearish = (
        is_high &
        prev_high_price.is_not_null() &
        (close > prev_high_price) &
        (macd_diff < prev_high_macd)
    )
    bullish = (
        is_low &
        prev_low_price.is_not_null() &
        (close < prev_low_price) &
        (macd_diff > prev_low_macd)
    )

    return pl.struct(**{
        f"{output_prefix}bearish": bearish.fill_null(False),
        f"{output_prefix}bullish": bullish.fill_null(False)
    })









