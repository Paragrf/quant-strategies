import time
import pandas as pd
import pytest
from src.data._cache import StockCache


@pytest.fixture
def cache(tmp_path):
    db = str(tmp_path / 'test.db')
    return StockCache(db_path=db)


# --- stock_cache ---

def test_get_stock_returns_none_when_empty(cache):
    assert cache.get_stock('600036') is None


def test_set_and_get_stock(cache):
    data = {'code': '600036', 'price': 35.12, 'name': '招商银行'}
    cache.set_stock('600036', data)
    result = cache.get_stock('600036')
    assert result == data


def test_get_stock_returns_none_after_ttl(cache):
    data = {'code': '600036', 'price': 35.12}
    cache.set_stock('600036', data)
    import sqlite3
    with sqlite3.connect(cache._db_path) as conn:
        conn.execute('UPDATE stock_cache SET updated_at = ? WHERE code = ?',
                     (time.time() - 7200, '600036'))
    assert cache.get_stock('600036') is None


# --- hist_cache ---

def test_get_hist_returns_none_when_empty(cache):
    assert cache.get_hist('600036_90') is None


def test_set_and_get_hist(cache):
    df = pd.DataFrame({
        'date': pd.to_datetime(['2024-01-02', '2024-01-03']),
        'open': [10.0, 10.5],
        'close': [10.5, 10.3],
        'high': [10.8, 10.7],
        'low': [9.9, 10.2],
        'volume': [100000.0, 90000.0],
    })
    cache.set_hist('600036_90', df)
    result = cache.get_hist('600036_90')
    assert result is not None
    assert len(result) == 2
    assert list(result.columns) == ['date', 'open', 'close', 'high', 'low', 'volume']
    assert pd.api.types.is_datetime64_any_dtype(result['date'])


# --- market_overview_cache ---

def test_get_market_overview_returns_none_when_empty(cache):
    assert cache.get_market_overview() is None


def test_set_and_get_market_overview(cache):
    data = {'rising_stocks': 150, 'falling_stocks': 100, 'rising_ratio': 60.0}
    cache.set_market_overview(data)
    result = cache.get_market_overview()
    assert result['rising_stocks'] == 150
    assert abs(result['rising_ratio'] - 60.0) < 0.001


def test_get_market_overview_returns_none_after_ttl(cache):
    cache.set_market_overview({'rising_stocks': 150})
    import sqlite3
    with sqlite3.connect(cache._db_path) as conn:
        conn.execute('UPDATE market_overview_cache SET updated_at = ? WHERE id = 1',
                     (time.time() - 600,))
    assert cache.get_market_overview() is None


# --- ma_reversal_results ---

def test_save_ma_reversal_results_returns_count(cache):
    signals = [
        {
            'code': '600036', 'name': '招商银行',
            'signal_score': 75.5, 'triggered_ma': 'MA120',
            'ma_proximity_pct': -1.5, 'drawdown_pct': -12.3,
            'vol_ratio': 0.55, 'vol_narrow_ratio': 0.60,
        },
        {
            'code': '000858', 'name': '五粮液',
            'signal_score': 68.0, 'triggered_ma': 'MA250',
            'ma_proximity_pct': 2.1, 'drawdown_pct': -11.0,
            'vol_ratio': 0.70, 'vol_narrow_ratio': 0.72,
        },
    ]
    n = cache.save_ma_reversal_results(signals, '2026-04-16')
    assert n == 2


def test_save_ma_reversal_results_upsert(cache):
    signal = [{
        'code': '600036', 'name': '招商银行',
        'signal_score': 75.5, 'triggered_ma': 'MA120',
        'ma_proximity_pct': -1.5, 'drawdown_pct': -12.3,
        'vol_ratio': 0.55, 'vol_narrow_ratio': 0.60,
    }]
    cache.save_ma_reversal_results(signal, '2026-04-16')
    # 同 scan_date + code 再次写入不报错，返回行数
    n = cache.save_ma_reversal_results(signal, '2026-04-16')
    assert n == 1
