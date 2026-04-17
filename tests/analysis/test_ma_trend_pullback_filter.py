# tests/analysis/test_ma_trend_pullback_filter.py
import numpy as np
import pandas as pd
import pytest
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter


def _make_uptrend_hist(n: int = 400, pullback_pct: float = -0.03) -> pd.DataFrame:
    """
    构造能通过斜率和穿越过滤的合成数据：
    - 价格整体线性上涨（MA 有正斜率）
    - 近 cross_window 日内有多次穿越
    - 当前价在均线下方 pullback_pct（负值）
    - 成交量回踩段缩量（vol_ratio ≈ 0.5）
    """
    dates = pd.date_range('2023-01-01', periods=n, freq='B')
    # 线性上涨基底：从 8 涨到 12（MA120 约 10，MA250 约 9.5）
    base = np.linspace(8.0, 12.0, n)

    # 近 cross_window(60)+1 = 61 日，制造 3 次穿越 MA120
    # 用 sin 波制造穿越
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone

    # 让最后一天价格低于 MA120（pullback_pct）
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * (1 + pullback_pct)

    # 缩量：回踩近 5 日均量 = 50，前 20 日均量 = 100
    volumes = np.ones(n) * 100.0
    volumes[-5:] = 50.0

    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })


def test_proximity_min_filters_deep_pullback():
    """回踩超过 proximity_min(-3%) 应被过滤返回 None。"""
    f = MATrendPullbackFilter(proximity_min=-0.03)
    hist = _make_uptrend_hist(pullback_pct=-0.06)   # 跌 6%，超过限制
    result = f._analyze_stock(hist)
    assert result is None


def test_proximity_min_passes_shallow_pullback():
    """回踩在 proximity_min(-3%) 范围内应通过。"""
    f = MATrendPullbackFilter(proximity_min=-0.03)
    hist = _make_uptrend_hist(pullback_pct=-0.02)   # 跌 2%，在限制内
    result = f._analyze_stock(hist)
    # 注意：此数据不一定通过所有过滤（斜率/穿越），仅验证 proximity_min 不是拦截方
    # 用 proximity_min 极小值确保只测深度限制
    f2 = MATrendPullbackFilter(proximity_min=-0.03, min_slope_pct=0.0, min_cross_count=0)
    result2 = f2._analyze_stock(hist)
    assert result2 is not None
    assert result2['proximity_pct'] > -3.0


def test_volume_ratio_max_filters_high_volume():
    """放量回踩（vol_ratio >= volume_ratio_max）应被过滤。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98   # 回踩 2%

    # 放量：近 5 日 = 前 20 日均量（vol_ratio = 1.0 >= 0.8）
    volumes = np.ones(400) * 100.0
    volumes[-5:] = 100.0               # 不缩量

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    assert f._analyze_stock(hist) is None


def test_volume_ratio_max_passes_low_volume():
    """缩量回踩（vol_ratio < volume_ratio_max）应通过成交量过滤。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98

    volumes = np.ones(400) * 100.0
    volumes[-5:] = 50.0                # vol_ratio = 0.5 < 0.8

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    result = f._analyze_stock(hist)
    assert result is not None


def test_volume_filter_skipped_when_no_volume_column():
    """hist 无 volume 列时，成交量过滤静默跳过，不报错。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        # 故意不包含 volume 列
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    result = f._analyze_stock(hist)
    assert result is not None  # 无 volume 列时不过滤


def test_analyze_stock_returns_ma_window():
    """_analyze_stock 应在结果中返回 ma_window（整数，等于触发均线的窗口大小）。"""
    f = MATrendPullbackFilter(min_slope_pct=0.0, min_cross_count=0, proximity_min=-0.10)
    hist = _make_uptrend_hist(pullback_pct=-0.02)
    result = f._analyze_stock(hist)
    assert result is not None
    assert 'ma_window' in result
    assert result['ma_window'] in (120, 250)


def test_proximity_min_default_is_minus_003():
    """proximity_min 默认值应为 -0.03。"""
    f = MATrendPullbackFilter()
    assert f.proximity_min == -0.03
