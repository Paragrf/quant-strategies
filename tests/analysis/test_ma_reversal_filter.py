import numpy as np
import pandas as pd
import pytest
from src.analysis.ma_reversal_filter import MAReversalFilter


def _make_passing_hist(n: int = 300) -> pd.DataFrame:
    """
    构造一条能通过全部5项过滤的合成历史数据：
    - 价格整体在 10.0，n-50 处有一个高点 12.0（回落 16.7%）
    - MA120 ≈ 10.017（当前价 10.0 偏差 < 0.2%，在 3% 容忍范围内）
    - 近 7 日成交量 = 50，长期均量 = 100（vol_ratio = 0.5 < 0.8）
    - 近 10 日收益率 std ≈ 0（价格恒定），历史 60 日 std > 0（有高点跳变）
    """
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    prices = np.ones(n) * 10.0
    prices[n - 50] = 12.0
    volumes = np.ones(n) * 100.0
    volumes[-7:] = 50.0
    return pd.DataFrame({
        'date': dates,
        'open': prices,
        'close': prices,
        'high': prices,
        'low': prices,
        'volume': volumes,
    })


def _make_failing_hist_no_drawdown(n: int = 300) -> pd.DataFrame:
    """当前价 = 60日最高价，回落幅度 = 0%，应被步骤2过滤。"""
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    prices = np.ones(n) * 10.0
    volumes = np.ones(n) * 100.0
    volumes[-7:] = 50.0
    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices, 'low': prices, 'volume': volumes,
    })


def _make_failing_hist_far_from_ma(n: int = 300) -> pd.DataFrame:
    """当前价比 MA120 低很多，超出 3% 容忍，应被步骤3过滤。"""
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    prices = np.ones(n) * 10.0
    prices[n - 50] = 12.0
    prices[-30:] = 8.0      # 当前价远低于 MA120
    volumes = np.ones(n) * 100.0
    volumes[-7:] = 50.0
    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices, 'low': prices, 'volume': volumes,
    })


def _make_failing_hist_high_volume(n: int = 300) -> pd.DataFrame:
    """近期成交量 = 长期均量（vol_ratio = 1.0），应被步骤4过滤。"""
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    prices = np.ones(n) * 10.0
    prices[n - 50] = 12.0
    volumes = np.ones(n) * 100.0  # 近期无量缩
    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices, 'low': prices, 'volume': volumes,
    })


def _make_failing_hist_high_volatility(n: int = 300) -> pd.DataFrame:
    """近期波动大（std 不收窄），应被步骤5过滤。"""
    np.random.seed(0)
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    prices = np.ones(n) * 10.0
    prices[n - 50] = 12.0
    prices[-10:] = 10.0 + np.random.randn(10) * 1.0
    prices[-1] = 10.0  # 保持当前价在 MA 附近
    volumes = np.ones(n) * 100.0
    volumes[-7:] = 50.0
    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices, 'low': prices, 'volume': volumes,
    })


# ---- 正向测试 ----

def test_analyze_stock_passing_case():
    f = MAReversalFilter()
    result = f._analyze_stock(_make_passing_hist())
    assert result is not None
    assert result['triggered_ma'] in ('MA120', 'MA250', 'MA120+MA250')
    assert result['drawdown_pct'] < -10.0
    assert result['vol_ratio'] < 0.8
    assert result['vol_narrow_ratio'] < 0.8
    assert 0 < result['signal_score'] <= 105  # 最大 100 + 5 双均线


def test_signal_score_is_positive():
    f = MAReversalFilter()
    result = f._analyze_stock(_make_passing_hist())
    assert result is not None
    assert result['signal_score'] > 0


# ---- 负向测试 ----

def test_analyze_stock_filtered_no_drawdown():
    f = MAReversalFilter()
    assert f._analyze_stock(_make_failing_hist_no_drawdown()) is None


def test_analyze_stock_filtered_far_from_ma():
    f = MAReversalFilter()
    assert f._analyze_stock(_make_failing_hist_far_from_ma()) is None


def test_analyze_stock_filtered_high_volume():
    f = MAReversalFilter()
    assert f._analyze_stock(_make_failing_hist_high_volume()) is None


def test_analyze_stock_filtered_high_volatility():
    f = MAReversalFilter()
    assert f._analyze_stock(_make_failing_hist_high_volatility()) is None


def test_analyze_stock_too_short_history():
    """历史数据不足 max(ma_windows)=250 日时应返回 None。"""
    f = MAReversalFilter()
    short_hist = _make_passing_hist(n=100)
    assert f._analyze_stock(short_hist) is None


def test_dual_ma_triggered():
    """构造当前价同时在 MA120 和 MA250 附近的情形，应触发双均线标签。"""
    n = 300
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    # 全部价格等于 10.0，MA120≈MA250≈10.0，偏差约 0%
    prices = np.ones(n) * 10.0
    prices[n - 50] = 12.0  # 60日内高点
    volumes = np.ones(n) * 100.0
    volumes[-7:] = 50.0
    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices, 'low': prices, 'volume': volumes,
    })
    f = MAReversalFilter()
    result = f._analyze_stock(hist)
    assert result is not None
    # 两条均线都触发
    assert 'MA120' in result['triggered_ma'] and 'MA250' in result['triggered_ma']
    # 双均线加分后得分应高于无加分的最大值（85 是合理下界）
    assert result['signal_score'] > 85
