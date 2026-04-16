import pandas as pd
import pytest
from src.data._parser import (
    parse_gtimg_stock,
    parse_kline,
    calculate_financial_health,
    calculate_momentum,
)


def _make_gtimg(stock_code: str, overrides: dict = None) -> str:
    """构造最小可解析的 gtimg 响应字符串。"""
    parts = [''] * 60
    defaults: dict = {
        0: '1',        1: '测试股票',  2: stock_code,
        3: '35.12',    4: '34.87',    6: '1234567',
        7: '43210987', 22: '8.8',     23: '886000',
        25: '25200000',27: '2.5',     32: '0.72',
        39: '8.6',     46: '1.25',    53: '19.30',
        56: '2.8',
    }
    if overrides:
        defaults.update(overrides)
    for idx, val in defaults.items():
        parts[idx] = str(val)
    prefix = 'sh' if stock_code.startswith('6') else 'sz'
    return f'v_{prefix}{stock_code}="' + '~'.join(parts) + '";\n'


def test_parse_gtimg_stock_basic_fields():
    content = _make_gtimg('600036')
    result = parse_gtimg_stock(content, '600036')
    assert result['code'] == '600036'
    assert result['name'] == '测试股票'
    assert abs(result['price'] - 35.12) < 0.001
    assert abs(result['prev_close'] - 34.87) < 0.001
    assert abs(result['change_pct'] - 0.72) < 0.001


def test_parse_gtimg_stock_uses_field46_for_pb():
    content = _make_gtimg('600036', {16: '1.10', 46: '1.25'})
    result = parse_gtimg_stock(content, '600036')
    assert abs(result['pb_ratio'] - 1.25) < 0.001


def test_parse_gtimg_stock_uses_field39_for_pe():
    content = _make_gtimg('600036', {39: '8.6', 22: '9.0', 15: '9.5', 14: '10.0'})
    result = parse_gtimg_stock(content, '600036')
    assert abs(result['pe_ratio'] - 8.6) < 0.001


def test_parse_gtimg_stock_uses_field56_for_turnover_rate():
    content = _make_gtimg('600036', {27: '2.5', 56: '2.8'})
    result = parse_gtimg_stock(content, '600036')
    assert abs(result['turnover_rate'] - 2.8) < 0.001


def test_parse_gtimg_stock_calculates_dividend_yield():
    content = _make_gtimg('600036', {53: '19.30', 3: '35.12'})
    result = parse_gtimg_stock(content, '600036')
    assert result['dividend_yield'] is not None
    assert 5.0 < result['dividend_yield'] < 6.0


def test_parse_gtimg_stock_manual_dividend_overrides():
    content = _make_gtimg('600036')
    result = parse_gtimg_stock(content, '600036', manual_dividend_fn=lambda _: 7.5)
    assert abs(result['dividend_yield'] - 7.5) < 0.001


def test_parse_gtimg_stock_returns_empty_on_bad_content():
    result = parse_gtimg_stock('bad content', '600036')
    assert result == {}


def test_parse_gtimg_stock_filters_invalid_pe():
    content = _make_gtimg('600036', {39: '9999', 22: '8.8'})
    result = parse_gtimg_stock(content, '600036')
    assert abs(result['pe_ratio'] - 8.8) < 0.001


def test_parse_kline_returns_dataframe():
    kline_json = (
        'kline_dayqfq={"data":{"sh600036":{"qfqday":'
        '[["2024-01-02","10.00","10.50","10.80","9.90","100000"],'
        ' ["2024-01-03","10.50","10.30","10.70","10.20","90000"]]}}}'
    )
    df = parse_kline(kline_json, '600036', hist_days=10)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['date', 'open', 'close', 'high', 'low', 'volume']
    assert len(df) == 2


def test_parse_kline_returns_empty_on_bad_content():
    df = parse_kline('bad', '600036', hist_days=10)
    assert df.empty


def test_financial_health_base_score():
    score = calculate_financial_health(None, None, None, None)
    assert score == 50


def test_financial_health_good_values():
    score = calculate_financial_health(pb=0.8, div_yield=4.0, pe=15.0, turnover=2.0)
    assert score > 70


def test_financial_health_clamped_0_to_100():
    score = calculate_financial_health(pb=50.0, div_yield=0.0, pe=200.0, turnover=50.0)
    assert 0 <= score <= 100


def test_calculate_momentum_positive():
    prices = pd.DataFrame({'close': [10.0] * 5 + [11.0] * 15})
    m = calculate_momentum(prices, days=20)
    assert m > 0


def test_calculate_momentum_insufficient_data():
    prices = pd.DataFrame({'close': [10.0] * 5})
    m = calculate_momentum(prices, days=20)
    assert m == 0
