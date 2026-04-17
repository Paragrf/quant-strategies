# src/analysis/ma_trend_pullback_filter.py
import asyncio
import logging
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd

from src.data import AsyncStockDataFetcher

logger = logging.getLogger(__name__)


class MATrendPullbackFilter:
    """
    均线趋势回踩扫描器。

    筛选逻辑（顺序执行，任一步不通则跳过）：
      1. 过滤 ST/*ST 股票
      2. MA120 或 MA250 处于持续上升趋势
         （取最近 slope_window 日的均线序列做线性回归，标准化斜率 > min_slope_pct）
      3. 近 cross_window 日内，股价上穿/下穿该均线的次数 >= min_cross_count
         （说明价格在均线附近反复震荡）
      4. 当前收盘价 < 均线（回踩至均线下方）

    评分公式（满分 100，双均线同时触发额外 +5）：
      slope_score     = min(slope_pct / ref_slope, 1.0) × 40   # 均线上涨坡度
      cross_score     = min(cross_count / ref_cross, 1.0) × 40  # 穿越活跃度
      proximity_score = (1 - |偏差| / proximity_cap) × 20       # 接近均线程度
      dual_ma_bonus   = +5（同时触及 MA120 和 MA250）
    """

    _REF_SLOPE = 0.002       # 标准化斜率参照值（对应 slope_score 满分）
    _REF_CROSS = 6           # 穿越次数参照值（对应 cross_score 满分）
    _PROXIMITY_CAP = 0.10    # 偏差超过此值 proximity_score 归零

    def __init__(
        self,
        ma_windows: List[int] = None,
        slope_window: int = 30,
        min_slope_pct: float = 0.0003,
        cross_window: int = 60,
        min_cross_count: int = 2,
        hist_days: int = 400,
        proximity_min: float = -0.05,
        volume_ratio_max: float = 0.8,
    ) -> None:
        self.ma_windows = ma_windows or [120, 250]
        self.slope_window = slope_window
        self.min_slope_pct = min_slope_pct
        self.cross_window = cross_window
        self.min_cross_count = min_cross_count
        self.hist_days = hist_days
        self.proximity_min = proximity_min
        self.volume_ratio_max = volume_ratio_max

    def scan_stocks_sync(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
    ) -> List[Dict]:
        """同步入口，不可从异步上下文调用。"""
        try:
            return asyncio.run(self._scan_async(stock_codes, stock_name_map))
        except Exception as e:
            logger.error(f"均线趋势回踩扫描失败: {e}")
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
            logger.warning(f"均线趋势回踩检测失败 {code}: {e}")
            return None

    def _analyze_stock(self, hist: pd.DataFrame) -> Optional[Dict]:
        """
        执行步骤2-4过滤并计算评分。
        返回结果字典（不含 code/name），或 None。
        """
        closes = hist['close'].astype(float).values
        n = len(closes)

        if n < max(self.ma_windows):
            return None

        current = closes[-1]

        triggered: List[Tuple[int, float, float, int]] = []  # (window, slope_pct, proximity, cross_count)

        for window in self.ma_windows:
            if n < window + self.slope_window:
                continue

            # 构造最近 slope_window 个 MA 值用于斜率计算
            # i ∈ [1, slope_window]，每个 MA 都是 window 日均线
            ma_series = np.array([
                float(np.mean(closes[n - window - self.slope_window + i : n - window + i]))
                for i in range(1, self.slope_window + 1)
            ])
            # 当前 MA（最近 window 日）
            current_ma = float(np.mean(closes[-window:]))

            # Step 2: 均线持续上升（线性回归斜率标准化）
            x = np.arange(self.slope_window, dtype=float)
            slope, _ = np.polyfit(x, ma_series, 1)
            ma_mean = float(np.mean(ma_series))
            if ma_mean == 0:
                continue
            slope_pct = slope / ma_mean  # 每日标准化上涨幅度
            if slope_pct < self.min_slope_pct:
                continue

            # Step 3: 穿越次数统计（近 cross_window 日）
            # i ∈ [0, cross_window]，共 cross_window+1 个点
            window_closes = closes[-(self.cross_window + 1):]
            ma_vals = np.array([
                float(np.mean(closes[n - window - self.cross_window + i : n - window + i]))
                for i in range(self.cross_window + 1)
            ])
            diff = window_closes - ma_vals
            # 将恰好等于均线（diff==0）的点归并到前一个非零符号，避免横盘虚计穿越
            sign_arr = np.sign(diff).astype(float)
            sign_arr[sign_arr == 0] = np.nan
            sign_filled = pd.Series(sign_arr).ffill().fillna(1.0).values
            sign_changes = int(np.sum(np.diff(sign_filled) != 0))
            if sign_changes < self.min_cross_count:
                continue

            # Step 4: 当前价 < 均线
            if current >= current_ma:
                continue

            proximity = current / current_ma - 1  # 负值

            # Step 4a: 回踩深度不能超过 proximity_min
            if proximity < self.proximity_min:
                continue

            triggered.append((window, slope_pct, proximity, sign_changes))

        if not triggered:
            return None

        # 选最佳：slope_pct 最大的均线
        best = max(triggered, key=lambda t: t[1])
        best_window, best_slope_pct, best_proximity, best_cross = best

        # 评分
        slope_score = min(best_slope_pct / self._REF_SLOPE, 1.0) * 40.0
        cross_score = min(best_cross / self._REF_CROSS, 1.0) * 40.0
        prox_abs = abs(best_proximity)
        proximity_score = max(0.0, (1.0 - prox_abs / self._PROXIMITY_CAP)) * 20.0
        dual_ma_bonus = 5.0 if len(triggered) >= 2 else 0.0
        signal_score = slope_score + cross_score + proximity_score + dual_ma_bonus

        if len(triggered) >= 2:
            triggered_ma = '+'.join(f'MA{w}' for w, _, _, _ in triggered)
        else:
            triggered_ma = f'MA{best_window}'

        return {
            'signal_score': round(signal_score, 2),
            'triggered_ma': triggered_ma,
            'slope_pct': round(best_slope_pct * 100, 4),
            'cross_count': best_cross,
            'proximity_pct': round(best_proximity * 100, 2),
        }
