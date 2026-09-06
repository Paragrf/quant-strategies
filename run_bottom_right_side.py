# -*- coding: utf-8 -*-
from datetime import datetime
import logging
import sys
import unicodedata

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)

from src.data._cache import StockCache
from src.data.universe_fetcher import get_a_share
from src.analysis.bottom_right_side_filter import BottomRightSideFilter


_cache = StockCache()
stocks = get_a_share(_cache)

if not stocks:
    print('A股股票池获取失败（akshare 不可用且缓存为空）')
    sys.exit(1)

print(f'股票池: 共 {len(stocks)} 只')

flt = BottomRightSideFilter()
stock_codes = [s['code'] for s in stocks]
stock_name_map = {s['code']: s.get('name', '') for s in stocks}

print(f'开始扫描 {len(stock_codes)} 只股票...')
signals = flt.scan_stocks_sync(stock_codes, stock_name_map)

scan_date = datetime.now().strftime('%Y-%m-%d')
saved = _cache.save_bottom_right_side_results(signals, scan_date)
print(f'已保存 {saved} 条结果至 SQLite（scan_date={scan_date}）')

print(f'\n===== 底部右侧启动信号 {scan_date} =====')
print(f'触发信号: {len(signals)} 只\n')


def _dw(s: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)


def _ljust(s: str, w: int) -> str:
    return s + ' ' * max(0, w - _dw(s))


def _rjust(s: str, w: int) -> str:
    return ' ' * max(0, w - _dw(s)) + s


if signals:
    col = [8, 12, 8, 7, 8, 8, 9, 9, 8, 8, 8]
    hdr = ['代码', '名称', '得分', '底部天', '平台振幅', '回撤%', 'MA5斜率', '平台偏离%', '2日涨幅%', '前10日涨幅%', '量比']
    aln = ['l', 'l', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'r', 'r']

    def _row(cells):
        return ''.join(
            _ljust(c, w) if a == 'l' else _rjust(c, w)
            for c, w, a in zip(cells, col, aln)
        )

    print(_row(hdr))
    print('-' * sum(col))
    for sig in signals:
        print(_row([
            sig['code'],
            sig['name'],
            f"{sig['signal_score']:.1f}",
            str(sig['bottom_days']),
            f"{sig['base_range_pct']:.1f}%",
            f"{sig['drawdown_pct']:.1f}%",
            f"{sig['ma_short_slope_pct']:.2f}%",
            f"{sig['breakout_pct']:.1f}%",
            f"{sig['return_2d_pct']:.1f}%",
            f"{sig['pre_return_10d_pct']:.1f}%",
            f"{sig['volume_ratio']:.2f}",
        ]))
else:
    print('当前无符合条件的股票')
