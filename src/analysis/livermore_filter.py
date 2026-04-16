# src/analysis/livermore_filter.py
import asyncio
import logging
import re
import numpy as np
import pandas as pd
import aiohttp
import akshare as ak
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from src.data import AsyncStockDataFetcher

logger = logging.getLogger(__name__)


class LivermoreFilter:

    def scan_stocks_sync(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
        realtime_map: Optional[Dict[str, Dict]] = None,
    ) -> List[Dict]:
        """同步入口，不可从异步上下文调用。"""
        try:
            return asyncio.run(self._scan_async(stock_codes, stock_name_map, realtime_map))
        except Exception as e:
            logger.error(f"利弗莫尔扫描失败: {e}")
            return []

    async def _scan_async(
        self,
        stock_codes: List[str],
        stock_name_map: Dict[str, str],
        realtime_map: Optional[Dict[str, Dict]] = None,
    ) -> List[Dict]:
        fetcher = AsyncStockDataFetcher()
        fetcher.semaphore = asyncio.Semaphore(20)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._scan_one(session, fetcher, code, stock_name_map.get(code, ""), realtime_map)
                for code in stock_codes
            ]
            results = await asyncio.gather(*tasks)
        signals = [r for r in results if r is not None]
        signals.sort(key=lambda x: x["signal_score"], reverse=True)
        return signals

    async def _scan_one(
        self,
        session: aiohttp.ClientSession,
        fetcher: AsyncStockDataFetcher,
        code: str,
        name: str,
        realtime_map: Optional[Dict[str, Dict]] = None,
    ) -> Optional[Dict]:
        try:
            # 过滤 ST / *ST 股票
            if "ST" in name.upper():
                return None

            # P0-1: 市值/成交额硬过滤
            if realtime_map:
                rt = realtime_map.get(code)
                if not rt:
                    return None
                market_cap = rt.get('market_cap')  # 万元
                turnover = rt.get('turnover')       # 元
                if market_cap is None or market_cap < 300000:   # 30亿
                    return None
                if turnover is None or turnover < 30000000:     # 3000万
                    return None

            hist = await fetcher.get_stock_historical_data(
                session, code, hist_days=120
            )
            if hist is None or len(hist) < 60:
                return None

            # 跳过数据过旧的股票（停牌等）：最后K线距今超过7自然日
            if "date" in hist.columns:
                last_date = pd.to_datetime(hist["date"].iloc[-1]).date()
                if (datetime.now().date() - last_date).days > 7:
                    return None

            signal_types: List[str] = []
            signal_details: Dict = {}

            today_close = float(hist['close'].iloc[-1])

            for detect_fn, key in [
                (self._detect_pivotal_breakout, "pivotal_breakout"),
                (self._detect_continuation, "continuation"),
                (self._detect_top_test, "top_test"),
            ]:
                for lookback in range(3):
                    h = hist.iloc[: len(hist) - lookback] if lookback > 0 else hist
                    if len(h) < 60:
                        continue
                    result = detect_fn(h)
                    if result:
                        # 历史突破需验证今日价格仍站稳关键位，否则视为假突破
                        if lookback > 0 and not self._confirm_breakout_holds(key, result, today_close):
                            break
                        if "date" in h.columns:
                            result["breakout_date"] = str(h["date"].iloc[-1].date())
                        signal_types.append(key)
                        signal_details[key] = result
                        break

            if not signal_types:
                return None

            # P0-2: 涨停标记
            limit_tag = ''
            if realtime_map:
                rt = realtime_map.get(code, {})
                change_pct = rt.get('change_pct', 0.0)
                last_close = float(hist['close'].iloc[-1])
                last_high = float(hist['high'].iloc[-1])
                last_open = float(hist['open'].iloc[-1])
                limit_tag = self._compute_limit_tag(code, change_pct, last_close, last_high, last_open)

            signal_date = (
                str(hist["date"].iloc[-1].date())
                if "date" in hist.columns
                else datetime.now().strftime("%Y-%m-%d")
            )
            strength = len(signal_types)
            trend_score = self._compute_trend_score(hist)
            return {
                "code": code,
                "name": name,
                "signal_types": signal_types,
                "signal_details": signal_details,
                "signal_strength": strength,
                "trend_score": trend_score,
                "signal_score": self._compute_signal_score(strength, signal_details, trend_score),
                "signal_date": signal_date,
                "limit_tag": limit_tag,
            }
        except Exception as e:
            logger.warning(f"利弗莫尔检测失败 {code}: {e}")
            return None

    def _confirm_breakout_holds(self, signal_type: str, result: Dict, today_close: float) -> bool:
        """验证历史突破信号在今日仍然有效：
        1. 今日收盘未跌破原关键位（pivot / consolidation_high / hist_high）
        2. 今日收盘未较突破价回落超过 5%（防止突破后次日大跌的假突破）
        """
        if signal_type == "pivotal_breakout":
            key_level = result.get("pivot_price", 0)
        elif signal_type == "continuation":
            key_level = result.get("consolidation_high", 0)
        elif signal_type == "top_test":
            key_level = result.get("hist_high", 0)
        else:
            return True

        if key_level <= 0 or today_close <= key_level:
            return False

        # 今日入场性价比：相对突破价回落不超过 3%
        breakout_ref = result.get("breakout_price") or result.get("current_price", 0)
        if breakout_ref > 0 and today_close < breakout_ref * 0.97:
            return False

        return True

    def _compute_trend_score(self, hist: pd.DataFrame) -> float:
        """
        趋势分 0~1.5：
        - close > MA20 > MA60（多头排列）: +1.0
        - close > MA20（部分排列）:        +0.3
        - MA20 斜率向上（近10日）:         +0~0.5（按斜率比例）
        """
        closes = hist['close'].astype(float).values
        n = len(closes)
        if n < 60:
            return 0.0

        ma20 = float(np.mean(closes[-20:]))
        ma60 = float(np.mean(closes[-60:]))
        current = closes[-1]

        score = 0.0
        if current > ma20 and ma20 > ma60:
            score += 1.0
        elif current > ma20:
            score += 0.3

        # MA20 斜率：用 10 日前的 MA20 做对比
        if n >= 30:
            ma20_prev = float(np.mean(closes[-30:-10]))
            if ma20_prev > 0:
                slope = (ma20 - ma20_prev) / ma20_prev
                score += max(0.0, min(slope / 0.05, 0.5))  # 5% 涨幅对应满分 0.5，不允许负分

        return round(score, 2)

    def _compute_signal_score(self, signal_strength: int, signal_details: Dict, trend_score: float = 0.0) -> float:
        """
        signal_score = signal_strength
                     + trend_score                         # 趋势分，上限 1.5
                     + Σ min(volume_ratio / 3.0, 1.0)     # 量比贡献，上限 1 分/信号
                     + Σ min(breakout_pct / 0.05, 1.0)    # 突破幅度贡献，上限 1 分/信号
        """
        score = float(signal_strength) + trend_score
        for details in signal_details.values():
            vol_ratio = details.get("volume_ratio", 1.0)
            score += min(vol_ratio / 3.0, 1.0)

            if "breakout_price" in details and "pivot_price" in details:
                pct = details["breakout_price"] / details["pivot_price"] - 1
            elif "breakout_price" in details and "consolidation_high" in details:
                pct = details["breakout_price"] / details["consolidation_high"] - 1
            elif "current_price" in details and "hist_high" in details:
                pct = details["current_price"] / details["hist_high"] - 1
            else:
                pct = 0.0
            score += min(pct / 0.05, 1.0)

        return round(score, 2)

    def _compute_limit_tag(self, code: str, change_pct: float,
                           close: float, high: float, open_price: float) -> str:
        """计算涨停标记。change_pct 为涨跌幅百分比。"""
        limit_pct = 19.5 if code.startswith(('300', '688')) else 9.5
        if change_pct < limit_pct:
            return ''
        # 一字涨停: open ≈ close ≈ high
        if close > 0:
            is_yizi = (abs(open_price - close) / close < 0.001
                       and abs(high - close) / close < 0.001)
        else:
            is_yizi = False
        return '[一字涨停]' if is_yizi else '[涨停]'

    def check_market_weak(self, index_hist: pd.DataFrame) -> bool:
        """判断大盘是否偏弱：最新收盘 < MA20 则返回 True。数据不足返回 False。"""
        if index_hist is None or len(index_hist) < 20:
            return False
        closes = index_hist['close'].astype(float).values
        ma20 = float(np.mean(closes[-20:]))
        return bool(closes[-1] < ma20)

    def apply_sector_resonance(self, signals: List[Dict], industry_map: Dict[str, str]) -> List[Dict]:
        """信号内板块共振标记：同行业 >=2 只信号 → 加标记 + signal_score +0.5"""
        if not industry_map:
            for sig in signals:
                sig.setdefault('sector_tag', '')
            return signals

        from collections import Counter
        industry_counts = Counter()
        sig_industry = {}
        for sig in signals:
            ind = industry_map.get(sig['code'], '')
            sig_industry[sig['code']] = ind
            if ind:
                industry_counts[ind] += 1

        for sig in signals:
            ind = sig_industry.get(sig['code'], '')
            count = industry_counts.get(ind, 0)
            if ind and count >= 2:
                sig['sector_tag'] = f'[板块共振: {ind} ×{count}]'
                sig['signal_score'] = round(sig['signal_score'] + 0.5, 2)
            else:
                sig['sector_tag'] = ''

        return signals

    def fetch_ths_board_map(self) -> Dict[str, float]:
        """获取同花顺行业板块涨跌幅，返回 {板块名: 涨跌幅}。失败返回空字典。"""
        try:
            df = ak.stock_board_industry_summary_ths()
            return dict(zip(df['板块'], df['涨跌幅'].astype(float)))
        except Exception as e:
            logger.warning(f"THS板块数据获取失败: {e}")
            return {}

    @staticmethod
    def _normalize_industry(name: str) -> str:
        """去掉东财行业后缀 Ⅰ/Ⅱ/Ⅲ/Ⅳ"""
        return re.sub(r'[ⅠⅡⅢⅣ]$', '', name)

    def apply_sector_hot_tag(self, signals: List[Dict], industry_map: Dict[str, str],
                             ths_board_map: Dict[str, float]) -> List[Dict]:
        """板块涨幅 >= 3% 的标记为强势板块，signal_score +0.3"""
        if not ths_board_map or not industry_map:
            for sig in signals:
                sig.setdefault('sector_hot_tag', '')
            return signals

        for sig in signals:
            ind = industry_map.get(sig['code'], '')
            if not ind:
                sig.setdefault('sector_hot_tag', '')
                continue
            norm_ind = self._normalize_industry(ind)
            matched_name = None
            matched_pct = 0.0
            for board_name, pct in ths_board_map.items():
                if norm_ind in board_name or board_name in norm_ind:
                    matched_name = board_name
                    matched_pct = pct
                    break
            if matched_name and matched_pct >= 3.0:
                sig['sector_hot_tag'] = f'[强势板块: {matched_name} +{matched_pct}%]'
                sig['signal_score'] = round(sig['signal_score'] + 0.3, 2)
            else:
                sig.setdefault('sector_hot_tag', '')

        return signals

    def _detect_pivotal_breakout(self, hist: pd.DataFrame) -> Optional[Dict]:
        """类型1：关键点突破（Pivotal Point Breakout）"""
        if len(hist) < 25:
            return None
        closes = hist['close'].astype(float).values
        highs = hist['high'].astype(float).values
        lows = hist['low'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)

        best_box = None
        for end in range(n - 2, n - 5, -1):
            for window in range(20, 9, -1):
                start = end - window + 1
                if start < 0:
                    continue
                segment = closes[start:end + 1]
                median_p = float(np.median(segment))
                if median_p == 0:
                    continue
                if all(abs(p - median_p) / median_p <= 0.05 for p in segment):
                    best_box = (start, end)
                    break
            if best_box:
                break

        if best_box is None:
            return None

        box_start, box_end = best_box

        # 振幅校验：箱内每根 K 线振幅不超过 8%（排除假整理）
        box_closes = closes[box_start:box_end + 1]
        box_highs = highs[box_start:box_end + 1]
        box_lows = lows[box_start:box_end + 1]
        if not all(
            (box_highs[i] - box_lows[i]) / box_closes[i] <= 0.08
            for i in range(len(box_closes))
        ):
            return None

        pivot_price = float(max(box_highs))

        breakout_price = float(closes[-1])
        if breakout_price <= pivot_price * 1.02:
            return None

        breakout_vol = float(volumes[-1])
        avg_vol = float(np.mean(volumes[max(0, n - 21):n - 1]))
        if avg_vol == 0 or breakout_vol < avg_vol * 2.0:
            return None

        return {
            'breakout_price': round(breakout_price, 3),
            'pivot_price': round(pivot_price, 3),
            'volume_ratio': round(breakout_vol / avg_vol, 2),
        }

    def _detect_continuation(self, hist: pd.DataFrame) -> Optional[Dict]:
        """类型2：趋势延续买点（Continuation Point）"""
        if len(hist) < 60:
            return None
        closes = hist['close'].astype(float).values
        highs = hist['high'].astype(float).values
        lows = hist['low'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)

        # 阶段高点：过去 30~60 日内最高收盘价
        win_start = max(0, n - 60)
        win_end = n - 30
        if win_end <= win_start:
            return None

        local_idx = int(np.argmax(closes[win_start:win_end]))
        stage_high_idx = win_start + local_idx
        stage_high_price = float(closes[stage_high_idx])

        # 趋势确认：阶段高点 > 30日前价格 × 1.1
        trend_ref_idx = max(0, stage_high_idx - 30)
        if stage_high_price <= closes[trend_ref_idx] * 1.1:
            return None

        # 整理期：stage_high_idx+1 起，连续低量天数（5~20）
        consol_start = stage_high_idx + 1
        if consol_start >= n - 1:
            return None

        pre_high_avg_vol = float(
            np.mean(volumes[max(0, stage_high_idx - 20):stage_high_idx])
        )
        if pre_high_avg_vol == 0:
            return None

        consol_len = 0
        for i in range(consol_start, min(consol_start + 20, n - 1)):
            if volumes[i] < pre_high_avg_vol * 0.8:
                consol_len += 1
            else:
                break

        if consol_len < 5:
            return None

        consol_end = consol_start + consol_len - 1

        # 振幅校验：整理期每根 K 线振幅不超过 8%（排除假整理）
        consol_closes = closes[consol_start:consol_end + 1]
        consol_highs = highs[consol_start:consol_end + 1]
        consol_lows = lows[consol_start:consol_end + 1]
        if not all(
            (consol_highs[i] - consol_lows[i]) / consol_closes[i] <= 0.08
            for i in range(len(consol_closes))
        ):
            return None

        consol_high = float(max(consol_highs))
        consol_avg_vol = float(np.mean(volumes[consol_start:consol_end + 1]))

        # 突破：最后一日 > 整理高点 + 放量
        breakout_price = float(closes[-1])
        if breakout_price <= consol_high * 1.01:
            return None

        breakout_vol = float(volumes[-1])
        avg_vol = float(np.mean(volumes[max(0, n - 21):n - 1]))
        if avg_vol == 0 or breakout_vol < avg_vol * 2.0:
            return None

        return {
            'breakout_price': round(breakout_price, 3),
            'consolidation_high': round(consol_high, 3),
            'volume_ratio': round(breakout_vol / avg_vol, 2),
            'consolidation_vol_ratio': round(consol_avg_vol / pre_high_avg_vol, 2),
        }

    def _detect_top_test(self, hist: pd.DataFrame) -> Optional[Dict]:
        """类型3：历史高点测试买点（Top Test）"""
        if len(hist) < 30:
            return None
        closes = hist['close'].astype(float).values
        highs = hist['high'].astype(float).values
        lows = hist['low'].astype(float).values
        volumes = hist['volume'].astype(float).values
        n = len(closes)

        # 历史高点（排除最后一日）：用最高价而非收盘价
        hist_high = float(max(highs[:-1]))

        # 测试期：最后一日之前的5日
        # 要求：high 未假突破历史高点（≤ 1.03×），low 未大幅回落（≥ 0.95×）
        test_highs = highs[-6:-1]
        test_lows = lows[-6:-1]
        if not all(h <= hist_high * 1.03 for h in test_highs):
            return None
        if not all(l >= hist_high * 0.95 for l in test_lows):
            return None

        # 测试期量能 vs 基准（测试期前20日）
        baseline_start = max(0, n - 26)
        baseline_end = n - 6
        if baseline_end <= baseline_start:
            return None
        baseline_vol = float(np.mean(volumes[baseline_start:baseline_end]))
        if baseline_vol == 0:
            return None

        test_vol_avg = float(np.mean(volumes[-6:-1]))
        if test_vol_avg >= baseline_vol * 0.8:
            return None

        # 突破：最后一日 > hist_high + 放量
        current_price = float(closes[-1])
        if current_price <= hist_high:
            return None

        breakout_vol = float(volumes[-1])
        avg_vol = float(np.mean(volumes[max(0, n - 21):n - 1]))
        if avg_vol == 0 or breakout_vol < avg_vol * 2.0:
            return None

        return {
            'current_price': round(current_price, 3),
            'hist_high': round(hist_high, 3),
            'volume_ratio': round(breakout_vol / avg_vol, 2),
            'test_vol_ratio': round(test_vol_avg / baseline_vol, 2),
        }
