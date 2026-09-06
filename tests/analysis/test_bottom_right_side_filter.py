import numpy as np
import pandas as pd

from src.analysis.bottom_right_side_filter import BottomRightSideFilter


def _frame(prices, volumes=None):
    if volumes is None:
        volumes = np.ones(len(prices)) * 100.0
    return pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=len(prices), freq='B'),
        'open': prices,
        'close': prices,
        'high': prices,
        'low': prices,
        'volume': volumes,
    })


def _make_passing_hist(n=400):
    prices = np.ones(n) * 15.0
    prices[300] = 20.0  # 平台前高点，确认底部回撤
    prices[338:360] = 10.5  # 在底部价格带停留 22 日
    prices[360:380] = 11.0
    prices[380:398] = 12.0  # 窄幅平台上沿
    prices[398:] = [12.05, 12.2]  # 最近两日刚开始启动

    volumes = np.ones(n) * 100.0
    volumes[398:] = 140.0  # 启动阶段温和放量
    return _frame(prices, volumes)


def test_analyze_stock_passing_case():
    result = BottomRightSideFilter()._analyze_stock(_make_passing_hist())

    assert result is not None
    assert result['bottom_days'] >= 18
    assert result['drawdown_pct'] <= -20.0
    assert result['breakout_pct'] <= 3.0
    assert 1.0 <= result['return_2d_pct'] <= 8.0
    assert result['pre_return_10d_pct'] <= 5.0
    assert result['volume_ratio'] >= 1.10
    assert 0 < result['signal_score'] <= 100


def test_analyze_stock_filtered_without_bottom_drawdown():
    prices = np.ones(400) * 12.0
    prices[338:360] = 10.5
    prices[360:380] = 11.0
    prices[380:398] = 12.0
    prices[398:] = [12.05, 12.2]
    volumes = np.ones(400) * 100.0
    volumes[398:] = 140.0

    assert BottomRightSideFilter()._analyze_stock(_frame(prices, volumes)) is None


def test_analyze_stock_filtered_with_sharply_shrinking_volume():
    hist = _make_passing_hist()
    hist.loc[398:, 'volume'] = 70.0

    assert BottomRightSideFilter()._analyze_stock(hist) is None


def test_analyze_stock_allows_first_move_before_volume_expansion():
    hist = _make_passing_hist()
    hist.loc[398:, 'volume'] = 82.0

    result = BottomRightSideFilter()._analyze_stock(hist)
    assert result is not None
    assert result['volume_ratio'] < 1.0


def test_analyze_stock_filtered_after_extended_rise():
    hist = _make_passing_hist()
    hist.loc[398:, 'close'] = [13.0, 14.0]

    assert BottomRightSideFilter()._analyze_stock(hist) is None


def test_analyze_stock_filtered_too_short_history():
    prices = np.ones(100) * 10.0
    assert BottomRightSideFilter()._analyze_stock(_frame(prices)) is None
