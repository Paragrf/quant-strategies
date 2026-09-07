# tests/test_backtest.py
import numpy as np
import pytest


def test_find_exit_recovery():
    """价格在 k=1 日收复均线，应返回 ('recovery', closes[t+1])。"""
    from run_backtest import _find_exit
    n = 200
    closes = np.ones(n) * 10.0
    t = 150
    closes[t] = 9.8           # 买入价（略低于均线）
    closes[t + 1] = 10.5      # 次日收复（10.5 >= MA120 ≈ 10.0）
    sell, reason = _find_exit(closes, t, window=120, max_hold_days=10, stop_loss=-0.10)
    assert reason == 'recovery'
    assert sell == pytest.approx(10.5)


def test_find_exit_stop_loss():
    """价格在 k=1 日触发止损（跌幅 > 10%），应返回 ('stop_loss', closes[t+1])。"""
    from run_backtest import _find_exit
    n = 200
    closes = np.ones(n) * 10.0
    t = 150
    closes[t] = 10.0          # 买入价
    closes[t + 1] = 8.9       # 跌 11%，触发 -10% 止损
    sell, reason = _find_exit(closes, t, window=120, max_hold_days=10, stop_loss=-0.10)
    assert reason == 'stop_loss'
    assert sell == pytest.approx(8.9)


def test_find_exit_max_hold():
    """价格始终在均线下方且未触发止损，到期后应返回 ('max_hold', closes[t+max_hold_days])。"""
    from run_backtest import _find_exit
    n = 200
    closes = np.ones(n) * 10.0
    t = 150
    # 持有期内价格保持 9.7（低于 MA=10.0，但跌幅 3% < 止损 10%）
    for k in range(1, 6):
        closes[t + k] = 9.7
    sell, reason = _find_exit(closes, t, window=120, max_hold_days=5, stop_loss=-0.10)
    assert reason == 'max_hold'
    assert sell == pytest.approx(9.7)


def test_find_exit_stop_loss_before_recovery():
    """止损先于收复均线触发时，应返回止损而非收复。"""
    from run_backtest import _find_exit
    n = 200
    closes = np.ones(n) * 10.0
    t = 150
    closes[t] = 10.0
    closes[t + 1] = 8.9       # k=1：跌 11%，止损
    closes[t + 2] = 10.5      # k=2：收复（但已在 k=1 止损）
    sell, reason = _find_exit(closes, t, window=120, max_hold_days=10, stop_loss=-0.10)
    assert reason == 'stop_loss'
    assert sell == pytest.approx(8.9)
