# src/analysis/dividend_filter.py
"""
红利低吸策略 (Dividend Dip Strategy)

筛选逻辑：
  1. 标的池：中证红利(000922) + 上证红利(000015) 成分股合集
  2. 剔除 ST/*ST 股票
  3. 计算 MA120（120 日收盘均线，需 ≥180 日历日历史数据）
  4. 触发条件：当前收盘价 ≤ MA120 × (1 - threshold)，默认 threshold=0.10 即低于 10%
  5. 附带股息率（优先 dividend_override，次选 akshare 实时数据）
  6. 结果按距 MA120 折价幅度从大到小排序
"""

import asyncio
import logging
import time
import aiohttp
import akshare as ak
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from src.data import AsyncStockDataFetcher
from src.data._cache import StockCache

logger = logging.getLogger(__name__)

# 红利指数代码
_DIVIDEND_INDICES = {
    "中证红利": ("csindex", "000922"),   # ak.index_stock_cons_csindex
    "上证红利": ("cons",    "000015"),   # ak.index_stock_cons
}

# 历史数据所需日历日（确保拿到 ≥120 个交易日）
_HIST_CALENDAR_DAYS = 200


_UNIVERSE_CACHE_KEY = "dividend_universe"


def get_dividend_universe(force_refresh: bool = False) -> Dict[str, str]:
    """
    返回 {stock_code: stock_name} 的红利股票池。
    合并中证红利(000922)和上证红利(000015)成分股，去重。
    结果缓存到 SQLite，TTL 30 天；force_refresh=True 强制重新拉取。
    """
    cache = StockCache()

    if not force_refresh:
        cached = cache.get_stock_list(_UNIVERSE_CACHE_KEY)
        if cached is not None:
            result = {s['code']: s['name'] for s in cached}
            logger.info(f"红利成分股命中缓存，共 {len(result)} 只")
            return result

    stocks: Dict[str, str] = {}

    # 中证红利 000922 — csindex 接口（SSL 偶发断连，最多重试3次）
    for attempt in range(3):
        try:
            df = ak.index_stock_cons_csindex(symbol="000922")
            for _, row in df.iterrows():
                code = str(row["成分券代码"]).zfill(6)
                stocks[code] = str(row["成分券名称"])
            logger.info(f"中证红利 000922: {len(df)} 只")
            break
        except Exception as e:
            if attempt < 2:
                logger.warning(f"获取中证红利成分股失败（第{attempt+1}次），2秒后重试: {e}")
                time.sleep(2)
            else:
                logger.error(f"获取中证红利成分股失败（已重试3次）: {e}")

    # 上证红利 000015 — cons 接口（SSL 偶发断连，最多重试3次）
    for attempt in range(3):
        try:
            df = ak.index_stock_cons(symbol="000015")
            for _, row in df.iterrows():
                code = str(row["品种代码"]).zfill(6)
                stocks[code] = str(row["品种名称"])
            logger.info(f"上证红利 000015: {len(df)} 只（合并后共 {len(stocks)} 只）")
            break
        except Exception as e:
            if attempt < 2:
                logger.warning(f"获取上证红利成分股失败（第{attempt+1}次），2秒后重试: {e}")
                time.sleep(2)
            else:
                logger.error(f"获取上证红利成分股失败（已重试3次）: {e}")

    if stocks:
        cache.set_stock_list(
            _UNIVERSE_CACHE_KEY,
            [{'code': c, 'name': n} for c, n in stocks.items()],
            data_source='akshare.index_stock_cons_csindex(000922)+index_stock_cons(000015)',
        )
        logger.info(f"红利成分股已写入缓存，共 {len(stocks)} 只")

    return stocks


class DividendDipFilter:
    """红利低吸筛选器"""

    def __init__(self, threshold: float = 0.10):
        """
        Args:
            threshold: 低于 MA120 的最小幅度，默认 0.10（即 10%）
        """
        self.threshold = threshold

    # ------------------------------------------------------------------
    # 公共入口
    # ------------------------------------------------------------------

    def scan_sync(self, universe: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        同步入口。不可从异步上下文调用。

        Args:
            universe: {code: name}，若为 None 则自动获取红利指数成分股

        Returns:
            满足条件的股票列表，按折价幅度降序排列
        """
        if universe is None:
            universe = get_dividend_universe()
        try:
            return asyncio.run(self._scan_async(universe))
        except Exception as e:
            logger.error(f"红利低吸扫描失败: {e}")
            return []

    # ------------------------------------------------------------------
    # 内部异步逻辑
    # ------------------------------------------------------------------

    async def _scan_async(self, universe: Dict[str, str]) -> List[Dict]:
        fetcher = AsyncStockDataFetcher()
        fetcher.semaphore = asyncio.Semaphore(20)

        codes = list(universe.keys())
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._scan_one(session, fetcher, code, universe[code])
                for code in codes
            ]
            results = await asyncio.gather(*tasks)

        hits = [r for r in results if r is not None]
        # 折价幅度从大到小
        hits.sort(key=lambda x: x["discount_pct"], reverse=True)
        return hits

    async def _scan_one(
        self,
        session: aiohttp.ClientSession,
        fetcher: AsyncStockDataFetcher,
        code: str,
        name: str,
    ) -> Optional[Dict]:
        try:
            # 剔除 ST
            if "ST" in name.upper():
                return None

            hist = await fetcher.get_stock_historical_data(
                session, code, hist_days=_HIST_CALENDAR_DAYS
            )
            if hist is None or len(hist) < 120:
                return None

            # 跳过停牌（最后 K 线距今超过 7 个自然日）
            if "date" in hist.columns:
                last_date = pd.to_datetime(hist["date"].iloc[-1]).date()
                if (datetime.now().date() - last_date).days > 7:
                    return None

            close = hist["close"].astype(float)
            current_price = float(close.iloc[-1])
            ma120 = float(close.tail(120).mean())

            # 触发条件：当前价格低于 MA120 的 threshold 倍
            if current_price >= ma120 * (1.0 - self.threshold):
                return None

            discount_pct = round((ma120 - current_price) / ma120 * 100, 2)

            # 获取股息率（先看 override，再看基本面接口）
            dividend_yield = self._get_dividend_yield(code, current_price)

            signal_date = (
                str(hist["date"].iloc[-1].date())
                if "date" in hist.columns
                else datetime.now().strftime("%Y-%m-%d")
            )

            return {
                "code": code,
                "name": name,
                "current_price": round(current_price, 2),
                "ma120": round(ma120, 2),
                "discount_pct": discount_pct,       # 低于 MA120 的百分比
                "dividend_yield": dividend_yield,    # 股息率 %，-1 表示未知
                "signal_date": signal_date,
            }

        except Exception as e:
            logger.warning(f"红利低吸检测失败 {code}: {e}")
            return None

    # ------------------------------------------------------------------
    # 股息率获取
    # ------------------------------------------------------------------

    def _get_dividend_yield(self, code: str, current_price: float) -> float:
        """
        获取股息率（%）。
        优先使用 dividend_override；次选：取近一年内已实施的分红合计 / 当前价格。
        返回 -1 表示数据不可用。
        """
        from config.dividend_override import get_manual_dividend_yield, has_manual_override
        from datetime import timedelta

        if has_manual_override(code):
            return get_manual_dividend_yield(code)

        try:
            df = ak.stock_history_dividend_detail(symbol=code, indicator="分红")
            if df.empty:
                return -1.0
            done = df[df["进度"] == "实施"].copy()
            if done.empty:
                return -1.0
            done["除权除息日"] = pd.to_datetime(done["除权除息日"], errors="coerce")
            done = done.dropna(subset=["除权除息日"])
            if done.empty:
                return -1.0
            cutoff = pd.Timestamp(datetime.now() - timedelta(days=365))
            recent = done[done["除权除息日"] >= cutoff]
            if recent.empty:
                # 近一年无分红，取最近一次作为参考
                recent = done.sort_values("除权除息日", ascending=False).head(1)
            total_per_10 = float(recent["派息"].sum())
            per_share = total_per_10 / 10.0
            return round(per_share / current_price * 100, 2)
        except Exception as e:
            logger.debug(f"股息率获取失败 {code}: {e}")
            return -1.0
