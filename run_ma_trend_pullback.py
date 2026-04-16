# -*- coding: utf-8 -*-
from datetime import datetime
import logging, sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)

from src.data._cache import StockCache
from src.data.universe_fetcher import get_csi300

_cache = StockCache()
stocks = get_csi300(_cache)

if not stocks:
    print('沪深300成分股获取失败（akshare 不可用且缓存为空）')
    sys.exit(1)

print(f'股票池: 共 {len(stocks)} 只')

from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter

mf = MATrendPullbackFilter()
stock_codes = [s['code'] for s in stocks]
stock_name_map = {s['code']: s.get('name', '') for s in stocks}

print(f'开始扫描 {len(stock_codes)} 只股票...')
signals = mf.scan_stocks_sync(stock_codes, stock_name_map)

scan_date = datetime.now().strftime('%Y-%m-%d')

# 保存至 SQLite
saved = _cache.save_ma_trend_pullback_results(signals, scan_date)
print(f'已保存 {saved} 条结果至 SQLite（scan_date={scan_date}）')

print(f'\n===== 均线趋势回踩信号 {scan_date} =====')
print(f'触发信号: {len(signals)} 只\n')

import unicodedata

def _dw(s: str) -> int:
    """终端显示宽度：CJK 全角字符占 2 列。"""
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)

def _ljust(s: str, w: int) -> str:
    return s + ' ' * max(0, w - _dw(s))

def _rjust(s: str, w: int) -> str:
    return ' ' * max(0, w - _dw(s)) + s

if signals:
    COL = [8, 12, 16, 7, 10, 8, 8]
    HDR = ['代码', '名称', '触发均线', '得分', '均线斜率', '穿越次', '偏差%']
    ALN = ['l',   'l',   'l',       'r',   'r',      'r',    'r']

    def _row(cells):
        return ''.join(
            _ljust(c, w) if a == 'l' else _rjust(c, w)
            for c, w, a in zip(cells, COL, ALN)
        )

    print(_row(HDR))
    print('-' * sum(COL))
    for sig in signals:
        print(_row([
            sig['code'],
            sig['name'],
            sig['triggered_ma'],
            f"{sig['signal_score']:.1f}",
            f"{sig['slope_pct']:.4f}%",
            str(sig['cross_count']),
            f"{sig['proximity_pct']:.1f}%",
        ]))
else:
    print('当前无符合条件的股票')
