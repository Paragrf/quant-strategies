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
    均线趋势回踩扫描器，支持两种模式。

    公共筛选逻辑（两种模式均执行）：
      1. 过滤 ST/*ST 股票
      2. MA120 或 MA250 处于持续上升趋势
         （取最近 slope_window 日的均线序列做线性回归，标准化斜率 > min_slope_pct）
      3. 近 cross_window 日内，股价上穿/下穿该均线的次数 >= min_cross_count
      4. 当前收盘价 < 均线

    mode='proximity'（默认）— 贴近均线买点：
      奖励刚刚跌破均线、回踩浅的情形。
      评分（满分 105）：
        slope_score     = min(slope_pct / 0.002, 1.0) × 40
        cross_score     = min(cross_count / 6, 1.0) × 40
        proximity_score = (1 - |偏差| / 10%) × 20
        dual_ma_bonus   = +5

    mode='reversal' — 深度回踩反转买点：
      额外要求回踩幅度在 [pullback_min, pullback_max] 区间内、
      价格已掉头向上、成交量开始放大。
      评分（满分 105）：
        slope_score         = min(slope_pct / 0.002, 1.0) × 30
        pullback_depth_score = 帐篷函数，中点 (pullback_min+pullback_max)/2 得满分 × 25
        momentum_score      = min(近 momentum_days 日涨幅 / 5%, 1.0) × 25
        vol_expand_score    = min((vol_ratio - 1) / 1.0, 1.0) × 20
        dual_ma_bonus       = +5
    """

    _REF_SLOPE = 0.002       # 标准化斜率参照值（对应 slope_score 满分）
    _REF_CROSS = 6           # 穿越次数参照值（对应 cross_score 满分）
    _PROXIMITY_CAP = 0.10    # proximity 模式：偏差超过此值得 0 分
    _REF_MOMENTUM = 0.05     # reversal 模式：5% 涨幅对应 momentum_score 满分
    _REF_VOL_DELTA = 1.0     # reversal 模式：量能扩张比例超过 1（即翻倍）得满分

    def __init__(
        self,
        ma_windows: List[int] = None,
        slope_window: int = 30,
        min_slope_pct: float = 0.0003,
        cross_window: int = 60,
        min_cross_count: int = 2,
        hist_days: int = 400,
        mode: str = 'proximity',
        pullback_min: float = 0.05,
        pullback_max: float = 0.15,
        momentum_days: int = 5,
        vol_expand_ratio: float = 0.8,
    ) -> None:
        if mode not in ('proximity', 'reversal'):
            raise ValueError(f"mode 必须为 'proximity' 或 'reversal'，got {mode!r}")
        self.ma_windows = ma_windows or [120, 250]
        self.slope_window = slope_window
        self.min_slope_pct = min_slope_pct
        self.cross_window = cross_window
        self.min_cross_count = min_cross_count
        self.hist_days = hist_days
        self.mode = mode
        self.pullback_min = pullback_min
        self.pullback_max = pullback_max
        self.momentum_days = momentum_days
        self.vol_expand_ratio = vol_expand_ratio

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
        执行公共步骤（2-4）过滤，再按 mode 做额外条件检查和评分。
        返回结果字典（不含 code/name），或 None。
        """
        closes = hist['close'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)

        if n < max(self.ma_windows):
            return None

        current = closes[-1]

        triggered: List[Tuple[int, float, float, int]] = []  # (window, slope_pct, proximity, cross_count)

        for window in self.ma_windows:
            if n < window + self.slope_window:
                continue

            # 构造最近 slope_window 个 MA 值，最后一点恰好等于 current_ma
            # i ∈ [1, slope_window]，i=slope_window 对应 closes[n-window : n]
            ma_series = np.array([
                float(np.mean(closes[n - window - self.slope_window + i : n - window + i]))
                for i in range(1, self.slope_window + 1)
            ])
            # 当前 MA（最近 window 日），与 ma_series[-1] 相同
            current_ma = ma_series[-1]

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
            triggered.append((window, slope_pct, proximity, sign_changes))

        if not triggered:
            return None

        # 选最佳：slope_pct 最大的均线
        best = max(triggered, key=lambda t: t[1])
        best_window, best_slope_pct, best_proximity, best_cross = best

        dual_ma_bonus = 5.0 if len(triggered) >= 2 else 0.0
        triggered_ma = (
            '+'.join(f'MA{w}' for w, _, _, _ in triggered)
            if len(triggered) >= 2
            else f'MA{best_window}'
        )

        if self.mode == 'proximity':
            return self._score_proximity(
                best_slope_pct, best_proximity, best_cross, dual_ma_bonus, triggered_ma
            )
        else:
            return self._score_reversal(
                closes, volumes, n,
                best_slope_pct, best_proximity, best_cross, dual_ma_bonus, triggered_ma
            )

    def _score_proximity(
        self,
        slope_pct: float,
        proximity: float,
        cross_count: int,
        dual_ma_bonus: float,
        triggered_ma: str,
    ) -> Dict:
        """mode='proximity' 评分：奖励贴近均线、刚刚跌破的情形。"""
        slope_score = min(slope_pct / self._REF_SLOPE, 1.0) * 40.0
        cross_score = min(cross_count / self._REF_CROSS, 1.0) * 40.0
        prox_abs = abs(proximity)
        proximity_score = max(0.0, (1.0 - prox_abs / self._PROXIMITY_CAP)) * 20.0
        signal_score = slope_score + cross_score + proximity_score + dual_ma_bonus
        return {
            'signal_score': round(signal_score, 2),
            'triggered_ma': triggered_ma,
            'slope_pct': round(slope_pct * 100, 4),
            'cross_count': cross_count,
            'proximity_pct': round(proximity * 100, 2),
        }

    def _score_reversal(
        self,
        closes: np.ndarray,
        volumes: np.ndarray,
        n: int,
        slope_pct: float,
        proximity: float,
        cross_count: int,
        dual_ma_bonus: float,
        triggered_ma: str,
    ) -> Optional[Dict]:
        """
        mode='reversal' 额外条件 + 评分：
          Step 5 — 回踩幅度在 [pullback_min, pullback_max] 区间
          Step 6 — 价格已掉头向上（近 momentum_days 日涨幅 > 0）
          Step 7 — 成交量开始放大（近 5 日均量 / 近 20 日均量 >= vol_expand_ratio）
        """
        prox_abs = abs(proximity)

        # Step 5: 回踩深度范围
        if not (self.pullback_min <= prox_abs <= self.pullback_max):
            return None

        # Step 6: 价格掉头向上
        if n <= self.momentum_days:
            return None
        ref_close = closes[-(self.momentum_days + 1)]
        if ref_close == 0:
            return None
        momentum = closes[-1] / ref_close - 1
        if momentum <= 0:
            return None

        # Step 7: 量能扩张
        if n < 20:
            return None
        vol_recent = float(np.mean(volumes[-5:]))
        vol_base = float(np.mean(volumes[-20:]))
        if vol_base == 0:
            return None
        vol_ratio = vol_recent / vol_base
        if vol_ratio < self.vol_expand_ratio:
            return None

        # 评分
        slope_score = min(slope_pct / self._REF_SLOPE, 1.0) * 30.0

        # 帐篷函数：中点得满分，两端得 0
        midpoint = (self.pullback_min + self.pullback_max) / 2.0
        half_width = (self.pullback_max - self.pullback_min) / 2.0
        pullback_depth_score = max(0.0, 1.0 - abs(prox_abs - midpoint) / half_width) * 25.0

        momentum_score = min(momentum / self._REF_MOMENTUM, 1.0) * 25.0
        vol_expand_score = min((vol_ratio - 1.0) / self._REF_VOL_DELTA, 1.0) * 20.0
        signal_score = slope_score + pullback_depth_score + momentum_score + vol_expand_score + dual_ma_bonus

        return {
            'signal_score': round(signal_score, 2),
            'triggered_ma': triggered_ma,
            'slope_pct': round(slope_pct * 100, 4),
            'cross_count': cross_count,
            'proximity_pct': round(proximity * 100, 2),
            'momentum_pct': round(momentum * 100, 2),
            'vol_ratio': round(vol_ratio, 3),
        }
