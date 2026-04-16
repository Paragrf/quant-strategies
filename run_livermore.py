# -*- coding: utf-8 -*-
from datetime import datetime
import logging, sys, os
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)

from src.data._cache import StockCache
from src.data.universe_fetcher import get_a_share, get_csi300

# 加载股票池（SQLite 缓存命中则直接用，过期则从 akshare 重拉）
_cache = StockCache()
stocks = get_a_share(_cache) or get_csi300(_cache)

if not stocks:
    print('股票池获取失败（akshare 不可用且缓存为空）')
    sys.exit(1)

print(f'股票池: 共 {len(stocks)} 只')

from src.analysis.livermore_filter import LivermoreFilter
from src.data.lhb_fetcher import LHBFetcher, apply_lhb_filter

lf = LivermoreFilter()
stock_codes = [s['code'] for s in stocks]
stock_name_map = {s['code']: s.get('name', '') for s in stocks}

print(f'开始扫描 {len(stock_codes)} 只股票...')
signals = lf.scan_stocks_sync(stock_codes, stock_name_map)

# 龙虎榜二次过滤
print('获取龙虎榜数据...')
lhb_map = LHBFetcher().get_lhb_map()
if not lhb_map:
    print('WARNING: 龙虎榜数据获取失败，跳过过滤')
else:
    signals = apply_lhb_filter(signals, lhb_map)
    signals.sort(key=lambda x: (x['signal_strength'], bool(x.get('lhb_tag')), x['signal_score']), reverse=True)

TYPES_CN = {'pivotal_breakout': '关键点突破', 'continuation': '趋势延续', 'top_test': '历史高点测试'}

# 详情字段中文名
DETAIL_KEYS_CN = {
    'breakout_price':     '突破价',
    'pivot_price':        '关键点',
    'consolidation_high': '整理高点',
    'hist_high':          '历史高点',
    'current_price':      '现价',
    'volume_ratio':       '量比',
    'breakout_pct':       '突破幅',
}

scan_date = datetime.now().strftime('%Y-%m-%d')
signal_date = signals[0]['signal_date'] if signals else scan_date

# 写入 SQLite
saved = _cache.save_livermore_results(signals, scan_date)
print(f'已保存 {saved} 条结果至 SQLite（scan_date={scan_date}）')

print(f'\n===== 利弗莫尔买点信号 {signal_date} =====')
print(f'触发信号: {len(signals)} 只\n')

import unicodedata

def _dw(s: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in ('W', 'F') else 1 for c in s)

def _ljust(s: str, w: int) -> str:
    return s + ' ' * max(0, w - _dw(s))

def _rjust(s: str, w: int) -> str:
    return ' ' * max(0, w - _dw(s)) + s

if signals:
    COL = [8, 10, 24, 4, 7, 12]
    HDR = ['代码', '名称', '信号类型', '强度', '得分', '信号日期']
    ALN = ['l',   'l',   'l',      'r',   'r',  'r']

    def _row(cells):
        return ''.join(
            _ljust(c, w) if a == 'l' else _rjust(c, w)
            for c, w, a in zip(cells, COL, ALN)
        )

    print(_row(HDR))
    print('-' * sum(COL))
    for sig in signals:
        type_str = '+'.join(TYPES_CN.get(t, t) for t in sig['signal_types'])
        print(_row([
            sig['code'], sig['name'], type_str,
            str(sig['signal_strength']),
            f"{sig['signal_score']:.2f}",
            sig['signal_date'],
        ]))

        # 附加标注（龙虎榜 / 涨停 / 板块共振 / 强势板块）
        tags = [v for k in ('lhb_tag', 'limit_tag', 'sector_tag', 'sector_hot_tag')
                if (v := sig.get(k))]
        if tags:
            print('        ' + '  '.join(tags))

        # 每个信号类型的详情
        for stype, detail in sig['signal_details'].items():
            parts = []
            for k, v in detail.items():
                if k == 'breakout_date':
                    continue
                label = DETAIL_KEYS_CN.get(k, k)
                parts.append(f'{label}:{v}')
            bd = detail.get('breakout_date', '')
            bd_str = f'  买点:{bd}' if bd else ''
            print(f"        [{TYPES_CN.get(stype, stype)}] {' '.join(parts)}{bd_str}")
else:
    print('当前无符合条件的股票')
