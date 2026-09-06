# -*- coding: utf-8 -*-
"""底部右侧早期启动策略。"""

import asyncio
import logging
from typing import Dict, List, Optional

import aiohttp
import numpy as np
import pandas as pd

from src.data import AsyncStockDataFetcher

logger = logging.getLogger(__name__)


class BottomRightSideFilter:
    """
    底部右侧早期启动扫描器。

    目标是找“刚开始涨一两天”的股票，而不是已经突破平台很久的股票：
      1. 过滤 ST/*ST 股票
      2. 最近 60 日之前的过渡期形成窄幅底部，并在底部价格带停留足够久
      3. 底部相对更早的高点回撤至少 20%
      4. 最近 2 日刚出现上涨，前 10 日没有提前大幅上涨
      5. MA5 开始向上，价格刚突破最近 5 日小高点
      6. 最近 2 日量能没有明显萎缩，且没有远超底部平台高点

    这是一个偏早的技术信号，优点是及时，代价是会包含更多假突破。
    """

    _REF_MA_SPREAD = 0.03  # MA5 相对 MA10 高 3% 时，短线转强分满
    _REF_MA_SLOPE = 0.03   # MA5 最近 3 日上涨 3% 时，短线拐头分满
    _REF_RETURN = 0.05     # 最近 2 日上涨 5% 时，启动分满

    def __init__(
        self,
        base_window: int = 60,
        transition_window: int = 2,
        drawdown_window: int = 120,
        min_drawdown: float = 0.20,
        max_base_range: float = 0.35,
        bottom_band_pct: float = 0.12,
        min_bottom_days: int = 18,
        pre_window: int = 10,
        max_pre_return: float = 0.05,
        min_transition_return: float = 0.002,
        max_transition_return: float = 0.08,
        local_breakout_window: int = 5,
        local_breakout_tolerance: float = 0.005,
        max_base_high_extension: float = 0.03,
        ma_slope_window: int = 3,
        min_ma_slope: float = 0.002,
        base_volume_window: int = 20,
        min_volume_ratio: float = 0.80,
        hist_days: int = 400,
        batch_size: int = 200,
        max_concurrent: int = 10,
    ) -> None:
        self.base_window = base_window
        self.transition_window = transition_window
        self.drawdown_window = drawdown_window
        self.min_drawdown = min_drawdown
        self.max_base_range = max_base_range
        self.bottom_band_pct = bottom_band_pct
        self.min_bottom_days = min_bottom_days
        self.pre_window = pre_window
        self.max_pre_return = max_pre_return
        self.min_transition_return = min_transition_return
        self.max_transition_return = max_transition_return
        self.local_breakout_window = local_breakout_window
        self.local_breakout_tolerance = local_breakout_tolerance
        self.max_base_high_extension = max_base_high_extension
        self.ma_slope_window = ma_slope_window
        self.min_ma_slope = min_ma_slope
        self.base_volume_window = base_volume_window
        self.min_volume_ratio = min_volume_ratio
        self.hist_days = hist_days
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent

    def scan_stocks_sync(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
    ) -> List[Dict]:
        """同步入口，不可从异步上下文调用。"""
        try:
            return asyncio.run(self._scan_async(stock_codes, stock_name_map))
        except Exception as e:
            logger.error(f"底部右侧早期启动扫描失败: {e}")
            return []

    async def _scan_async(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
    ) -> List[Dict]:
        fetcher = AsyncStockDataFetcher()
        fetcher.semaphore = asyncio.Semaphore(self.max_concurrent)
        unique_codes = list(dict.fromkeys(stock_codes))
        all_signals = []
        async with aiohttp.ClientSession() as session:
            for start in range(0, len(unique_codes), self.batch_size):
                batch = unique_codes[start:start + self.batch_size]
                tasks = [
                    self._scan_one(session, fetcher, code, stock_name_map.get(code, ''))
                    for code in batch
                ]
                results = await asyncio.gather(*tasks)
                all_signals.extend(r for r in results if r is not None)
                logger.info(
                    '底部右侧早期启动扫描进度: %d/%d',
                    min(start + len(batch), len(unique_codes)), len(unique_codes),
                )
        signals = all_signals
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
            logger.warning(f"底部右侧早期启动检测失败 {code}: {e}")
            return None

    def _analyze_stock(self, hist: pd.DataFrame) -> Optional[Dict]:
        """执行底部确认、早期转强和放量过滤并计算评分。"""
        closes = hist['close'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)
        required = max(
            self.base_window + self.transition_window + 1,
            self.drawdown_window + self.transition_window + 1,
            20 + self.ma_slope_window,
            self.transition_window + self.pre_window + 1,
            self.transition_window + self.base_volume_window,
        )
        if n < required:
            return None

        if (
            np.any(~np.isfinite(closes))
            or np.any(~np.isfinite(volumes))
            or closes[-1] <= 0
        ):
            return None

        current = closes[-1]
        base_end = n - self.transition_window
        base_start = base_end - self.base_window
        base = closes[base_start:base_end]
        base_low = float(np.min(base))
        base_high = float(np.max(base))
        if base_low <= 0 or base_high <= 0:
            return None

        # 底部形态：窄幅、低位停留。
        base_range = base_high / base_low - 1.0
        bottom_days = int(np.sum(base <= base_low * (1.0 + self.bottom_band_pct)))
        if base_range > self.max_base_range or bottom_days < self.min_bottom_days:
            return None

        # 底部必须相对更早的参考高点有明显回撤。
        reference = closes[base_end - self.drawdown_window:base_end]
        reference_high = float(np.max(reference))
        if reference_high <= 0:
            return None
        drawdown = base_low / reference_high - 1.0
        if drawdown > -self.min_drawdown:
            return None

        # 最近两天刚启动，且启动前十天没有先涨太多。
        trigger_start = n - self.transition_window
        pre_start = trigger_start - self.pre_window - 1
        pre_return = closes[trigger_start - 1] / closes[pre_start] - 1.0
        return_2d = current / closes[trigger_start - 1] - 1.0
        up_days = int(np.sum(np.diff(closes[trigger_start - 1:]) > 0))
        if pre_return > self.max_pre_return:
            return None
        if not self.min_transition_return <= return_2d <= self.max_transition_return:
            return None
        if current <= closes[-2] or up_days < 1:
            return None

        # 只要求突破最近小平台，不再等待突破 60 日平台高点。
        local_start = n - self.transition_window - self.local_breakout_window
        local_high = float(np.max(closes[local_start:trigger_start]))
        if local_high <= 0:
            return None
        local_breakout = current / local_high - 1.0
        if local_breakout < -self.local_breakout_tolerance:
            return None
        # 避免已经远离底部平台、变成追高信号。
        base_high_extension = current / base_high - 1.0
        if base_high_extension > self.max_base_high_extension:
            return None

        # 短线均线拐头：不再要求 MA20 已经站上 MA60。
        ma5 = float(np.mean(closes[-5:]))
        ma10 = float(np.mean(closes[-10:]))
        ma5_prev = float(np.mean(closes[-5 - self.ma_slope_window:-self.ma_slope_window]))
        if ma5 <= 0 or ma10 <= 0 or ma5_prev <= 0:
            return None
        ma_spread = ma5 / ma10 - 1.0
        ma_short_slope = ma5 / ma5_prev - 1.0
        if current < ma5 or ma5 <= ma10 or ma_short_slope < self.min_ma_slope:
            return None

        base_volume = float(np.mean(
            volumes[-self.transition_window - self.base_volume_window:-self.transition_window]
        ))
        recent_volume = float(np.mean(volumes[-self.transition_window:]))
        if base_volume <= 0:
            return None
        volume_ratio = recent_volume / base_volume
        if volume_ratio < self.min_volume_ratio:
            return None

        # 评分满分 100：底部 40 + 短线拐头 30 + 初动 20 + 放量 10。
        bottom_score = (
            min(bottom_days / max(self.min_bottom_days, 1), 1.0) * 15.0
            + max(0.0, 1.0 - base_range / self.max_base_range) * 15.0
            + min(abs(drawdown) / self.min_drawdown, 1.0) * 10.0
        )
        turn_score = (
            min(max(ma_spread, 0.0) / self._REF_MA_SPREAD, 1.0) * 15.0
            + min(max(ma_short_slope, 0.0) / self._REF_MA_SLOPE, 1.0) * 15.0
        )
        early_score = (
            max(0.0, 1.0 - abs(local_breakout) / max(self.local_breakout_tolerance, 0.03)) * 10.0
            + min(max(return_2d, 0.0) / self._REF_RETURN, 1.0) * 10.0
        )
        volume_score = min(volume_ratio / 2.0, 1.0) * 10.0
        signal_score = bottom_score + turn_score + early_score + volume_score

        return {
            'signal_score': round(signal_score, 2),
            'base_range_pct': round(base_range * 100, 2),
            'bottom_days': bottom_days,
            'drawdown_pct': round(drawdown * 100, 2),
            'ma_spread_pct': round(ma_spread * 100, 2),
            'ma_short_slope_pct': round(ma_short_slope * 100, 2),
            'breakout_pct': round(base_high_extension * 100, 2),
            'return_2d_pct': round(return_2d * 100, 2),
            'pre_return_10d_pct': round(pre_return * 100, 2),
            'local_breakout_pct': round(local_breakout * 100, 2),
            'volume_ratio': round(volume_ratio, 3),
        }
