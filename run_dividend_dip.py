# -*- coding: utf-8 -*-
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from src.analysis.dividend_filter import DividendDipFilter, get_dividend_universe

print('正在获取红利成分股...')
universe = get_dividend_universe()
print('成分股总数: {} 只'.format(len(universe)))

print('\n开始扫描红利低吸机会（阈值: 低于MA120超过10%）...')
flt = DividendDipFilter(threshold=0.10)
hits = flt.scan_sync(universe)

from datetime import datetime
from src.data._cache import StockCache
scan_date = datetime.now().strftime('%Y-%m-%d')
saved = StockCache().save_dividend_results(hits, scan_date)
print(f'已保存 {saved} 条结果至 SQLite（scan_date={scan_date}）')

print('\n===== 红利低吸筛选结果 =====')
print('触发信号: {} 只\n'.format(len(hits)))
import unicodedata

def _dw(s: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)

def _ljust(s: str, w: int) -> str:
    return s + ' ' * max(0, w - _dw(s))

def _rjust(s: str, w: int) -> str:
    return ' ' * max(0, w - _dw(s)) + s

if hits:
    COL = [8, 12, 8, 8, 8, 8, 14]
    HDR = ['代码', '名称', '现价', 'MA120', '折价%', '股息率%', '信号日期']
    ALN = ['l',   'l',   'r',   'r',    'r',    'r',     'r']

    def _row(cells):
        return ''.join(
            _ljust(c, w) if a == 'l' else _rjust(c, w)
            for c, w, a in zip(cells, COL, ALN)
        )

    print(_row(HDR))
    print('-' * sum(COL))
    for s in hits:
        dy = '{:.2f}'.format(s['dividend_yield']) if s['dividend_yield'] != -1 else 'N/A'
        print(_row([
            s['code'],
            s['name'],
            f"{s['current_price']:.2f}",
            f"{s['ma120']:.2f}",
            f"{s['discount_pct']:.2f}%",
            dy,
            s['signal_date'],
        ]))
else:
    print('当前无符合条件的股票')
