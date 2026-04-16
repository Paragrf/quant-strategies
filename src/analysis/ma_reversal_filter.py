# src/analysis/ma_reversal_filter.py
import asyncio
import logging
from typing import Dict, List, Optional

import aiohttp
import numpy as np
import pandas as pd

from src.data import AsyncStockDataFetcher

logger = logging.getLogger(__name__)


class MAReversalFilter:
    """
    均线支撑反转扫描器。

    筛选逻辑（顺序执行，任一步不通则跳过）：
      1. 过滤 ST/*ST 股票
      2. 从近 60 日高点回落幅度 >= min_drawdown
      3. 当前价在 MA120 或 MA250 的 ±ma_tolerance 范围内
      4. 近 vol_window 日均量 / 近 60 日均量 < vol_ratio_threshold（量缩）
      5. 近 10 日收益率标准差 / 近 60 日收益率标准差 < vol_narrow_threshold（波动收窄）

    评分公式（满分 100，双均线共振额外 +5）：
      signal_score = (1 - |偏差|/ma_tolerance) × 40   # 均线接近度
                   + (1 - vol_ratio) × 30              # 量缩幅度
                   + (1 - vol_narrow_ratio) × 30       # 波动收窄
                   + 5（若同时触发 MA120 和 MA250）
    """

    def __init__(
        self,
        ma_windows: List[int] = None,
        ma_tolerance: float = 0.03,
        min_drawdown: float = 0.10,
        vol_window: int = 7,
        vol_ratio_threshold: float = 0.80,
        vol_narrow_threshold: float = 0.80,
        hist_days: int = 300,
    ) -> None:
        self.ma_windows = ma_windows or [120, 250]
        self.ma_tolerance = ma_tolerance
        self.min_drawdown = min_drawdown
        self.vol_window = vol_window
        self.vol_ratio_threshold = vol_ratio_threshold
        self.vol_narrow_threshold = vol_narrow_threshold
        self.hist_days = hist_days

    def scan_stocks_sync(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
    ) -> List[Dict]:
        """同步入口，不可从异步上下文调用。"""
        try:
            return asyncio.run(self._scan_async(stock_codes, stock_name_map))
        except Exception as e:
            logger.error(f"均线反转扫描失败: {e}")
            return []

    async def _scan_async(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
    ) -> List[Dict]:
        fetcher = AsyncStockDataFetcher()
        fetcher.semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._scan_one(session, fetcher, code, stock_name_map.get(code, ''))
                for code in stock_codes
            ]
            results = await asyncio.gather(*tasks)
        signals = [r for r in results if r is not None]
        signals.sort(key=lambda x: x['signal_score'], reverse=True)
        return signals

    async def _scan_one(
        self,
        session: aiohttp.ClientSession,
        fetcher: AsyncStockDataFetcher,
        code: str,
        name: str,
    ) -> Optional[Dict]:
        try:
            if 'ST' in name.upper():
                return None

            hist = await fetcher.get_stock_historical_data(
                session, code, hist_days=self.hist_days
            )
            if hist is None or hist.empty:
                return None

            result = self._analyze_stock(hist)
            if result is None:
                return None

            return {'code': code, 'name': name, **result}
        except Exception as e:
            logger.warning(f"均线反转检测失败 {code}: {e}")
            return None

    def _analyze_stock(self, hist: pd.DataFrame) -> Optional[Dict]:
        """
        执行步骤2-5过滤并计算评分。
        返回结果字典（不含 code/name），或 None。
        """
        closes = hist['close'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)

        if n < max(self.ma_windows):
            return None

        current = closes[-1]

        # Step 2: 回落幅度
        high_60 = float(np.max(closes[-60:]))
        if high_60 == 0:
            return None
        drawdown = current / high_60 - 1
        if drawdown > -self.min_drawdown:
            return None

        # Step 3: 均线接近
        triggered: List[tuple] = []
        for window in self.ma_windows:
            if n < window:
                continue
            ma = float(np.mean(closes[-window:]))
            if ma == 0:
                continue
            proximity = current / ma - 1
            if abs(proximity) <= self.ma_tolerance:
                triggered.append((window, proximity))

        if not triggered:
            return None

        best_window, best_proximity = min(triggered, key=lambda t: abs(t[1]))

        # Step 4: 量缩确认
        if n < 60:
            return None
        recent_vol = float(np.mean(volumes[-self.vol_window:]))
        long_vol = float(np.mean(volumes[-60:]))
        if long_vol == 0:
            return None
        vol_ratio = recent_vol / long_vol
        if vol_ratio >= self.vol_ratio_threshold:
            return None

        # Step 5: 波动收窄
        if n < 61:
            return None
        returns = np.diff(closes) / closes[:-1]
        recent_std = float(np.std(returns[-10:]))
        hist_std = float(np.std(returns[-60:]))
        if hist_std == 0:
            return None
        vol_narrow_ratio = recent_std / hist_std
        if vol_narrow_ratio >= self.vol_narrow_threshold:
            return None

        # 评分
        ma_proximity_score = (1.0 - abs(best_proximity) / self.ma_tolerance) * 40.0
        vol_shrink_score = (1.0 - vol_ratio) * 30.0
        volatility_score = (1.0 - vol_narrow_ratio) * 30.0
        dual_ma_bonus = 5.0 if len(triggered) >= 2 else 0.0
        signal_score = ma_proximity_score + vol_shrink_score + volatility_score + dual_ma_bonus

        if len(triggered) >= 2:
            triggered_ma = '+'.join(f'MA{w}' for w, _ in triggered)
        else:
            triggered_ma = f'MA{best_window}'

        return {
            'signal_score': round(signal_score, 2),
            'triggered_ma': triggered_ma,
            'ma_proximity_pct': round(best_proximity * 100, 2),
            'drawdown_pct': round(drawdown * 100, 2),
            'vol_ratio': round(vol_ratio, 3),
            'vol_narrow_ratio': round(vol_narrow_ratio, 3),
        }
