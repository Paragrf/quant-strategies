# run_backtest.py
"""
动态退出回测脚本：对每只股票历史数据滚动回放，统计 MATrendPullbackFilter 信号的
动态三条件退出（收复均线/止损/到期强平）收益表现。

用法：
    python3 run_backtest.py [--max_hold_days N] [--stop_loss F] [--volume_filter on|off] [--proximity_min F]

示例：
    python3 run_backtest.py
    python3 run_backtest.py --max_hold_days 20 --stop_loss -0.03
"""

import argparse
import asyncio
import logging
import sys
import time
from typing import Dict, List, Tuple

import aiohttp
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

from src.data._cache import StockCache
from src.data.universe_fetcher import get_csi300
from src.data import AsyncStockDataFetcher
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='MA Trend Pullback 动态退出回测')
    p.add_argument('--max_hold_days', type=int, default=30,
                   help='最长持有天数（默认 30）')
    p.add_argument('--stop_loss', type=float, default=-0.045,
                   help='止损线，如 -0.045 表示亏损 4.5%% 时止损（默认 -0.045）')
    p.add_argument('--volume_filter', choices=['on', 'off'], default='on',
                   help='是否启用成交量缩量过滤（默认 on）')
    p.add_argument('--proximity_min', type=float, default=-0.03,
                   help='回踩深度下限（默认 -0.03）')
    return p.parse_args()


def _find_exit(
    closes: np.ndarray,
    t: int,
    window: int,
    max_hold_days: int,
    stop_loss: float,
) -> Tuple[float, str]:
    """
    从买入日 t 逐日寻找退出点。
    优先级：收复均线（止盈）> 止损 > 到期强平。
    返回 (sell_price, reason)，reason ∈ {'recovery', 'stop_loss', 'max_hold', 'end_of_data'}
    """
    buy = float(closes[t])
    n = len(closes)
    for k in range(1, max_hold_days + 1):
        if t + k >= n:
            return float(closes[-1]), 'end_of_data'
        price = float(closes[t + k])
        ma = float(np.mean(closes[t + k - window: t + k]))
        if price >= ma:
            return price, 'recovery'
        if buy > 0 and (price - buy) / buy < stop_loss:
            return price, 'stop_loss'
    return float(closes[t + max_hold_days]), 'max_hold'


def _backtest_one(
    hist: pd.DataFrame,
    f: MATrendPullbackFilter,
    max_hold_days: int,
    stop_loss: float,
) -> List[Dict]:
    """滚动回放，返回每个信号的 {'ret': float, 'reason': str} 列表。"""
    closes = hist['close'].astype(float).values
    n = len(closes)
    min_start = max(f.ma_windows) + max(f.slope_window, f.cross_window)
    records = []
    for t in range(min_start, n - max_hold_days):
        sub = hist.iloc[:t].reset_index(drop=True)
        result = f._analyze_stock(sub)
        if result is not None:
            buy = closes[t]
            if buy > 0:
                sell, reason = _find_exit(closes, t, result['ma_window'], max_hold_days, stop_loss)
                records.append({'ret': (sell - buy) / buy, 'reason': reason})
    return records


def _print_summary(
    records: List[Dict],
    max_hold_days: int,
    stop_loss: float,
    volume_filter: str,
    proximity_min: float,
) -> None:
    if not records:
        print('\n无有效信号，无法统计。')
        return
    rets = np.array([r['ret'] for r in records])
    reasons = [r['reason'] for r in records]
    n = len(rets)
    win_rate     = (rets > 0).mean() * 100
    avg_ret      = rets.mean() * 100
    med_ret      = np.median(rets) * 100
    max_loss     = rets.min() * 100
    recovery_pct = reasons.count('recovery') / n * 100
    stop_pct     = reasons.count('stop_loss') / n * 100
    expire_pct   = (reasons.count('max_hold') + reasons.count('end_of_data')) / n * 100
    print(f'\n===== 回测结果 (最长持有={max_hold_days}日 止损={stop_loss*100:.1f}% 成交量过滤={volume_filter} 深度限制={proximity_min*100:.1f}%) =====')
    print(f'信号总数:       {n}')
    print(f'胜率:           {win_rate:.1f}%')
    print(f'平均收益:      {avg_ret:+.2f}%')
    print(f'中位收益:      {med_ret:+.2f}%')
    print(f'最大单笔亏损:  {max_loss:+.2f}%')
    print(f'收复均线离场:  {recovery_pct:.1f}%')
    print(f'止损离场:       {stop_pct:.1f}%')
    print(f'到期强平:       {expire_pct:.1f}%')


async def _fetch_all_hist(stock_codes: List[str], hist_days: int) -> dict:
    fetcher = AsyncStockDataFetcher()
    fetcher.semaphore = asyncio.Semaphore(20)
    result = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetcher.get_stock_historical_data(session, code, hist_days=hist_days) for code in stock_codes]
        hists = await asyncio.gather(*tasks)
    for code, hist in zip(stock_codes, hists):
        if hist is not None and not hist.empty:
            result[code] = hist
    return result


def main() -> None:
    args = _parse_args()
    cache = StockCache()
    stocks = get_csi300(cache)
    if not stocks:
        print('沪深300成分股获取失败')
        sys.exit(1)

    volume_ratio = 0.8 if args.volume_filter == 'on' else 999.0
    f = MATrendPullbackFilter(
        proximity_min=args.proximity_min,
        volume_ratio_max=volume_ratio,
    )

    stock_codes = [s['code'] for s in stocks]
    hist_days = max(f.ma_windows) + max(f.slope_window, f.cross_window) + args.max_hold_days + 20
    hist_days = max(hist_days, 400)

    logger.info('获取 %d 只股票历史数据（%d 日）...', len(stock_codes), hist_days)
    t0 = time.time()
    hist_map = asyncio.run(_fetch_all_hist(stock_codes, hist_days))
    logger.info('数据获取完成，用时 %.1fs，有效 %d 只', time.time() - t0, len(hist_map))

    logger.info('开始回放信号...')
    all_records: List[Dict] = []
    for hist in hist_map.values():
        all_records.extend(_backtest_one(hist, f, args.max_hold_days, args.stop_loss))

    _print_summary(all_records, args.max_hold_days, args.stop_loss, args.volume_filter, args.proximity_min)


if __name__ == '__main__':
    main()
