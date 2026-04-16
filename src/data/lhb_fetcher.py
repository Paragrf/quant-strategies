# src/data/lhb_fetcher.py
# -*- coding: utf-8 -*-
"""
龙虎榜数据获取器
数据源: 东方财富 stock_lhb_detail_em
缓存策略: SQLite lhb_cache 表，当日有效（跨 0 点立即失效）
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List

import akshare as ak
import pandas as pd

from ._cache import StockCache

logger = logging.getLogger(__name__)


class LHBFetcher:
    """获取并缓存近3个交易日龙虎榜数据，聚合为 {code: {net_buy_total, appear_count, reasons}}"""

    def __init__(self) -> None:
        self._cache = StockCache()

    def _fetch_raw(self, start_date: str, end_date: str) -> pd.DataFrame:
        df = ak.stock_lhb_detail_em(start_date=start_date, end_date=end_date)
        if df is None or df.empty:
            return pd.DataFrame()
        return df

    def _aggregate(self, df: pd.DataFrame) -> Dict:
        """过滤跌相关原因（记录级别），聚合为 {code: {...}} 字典"""
        if df.empty:
            return {}

        mask = ~df['上榜原因'].str.contains('跌', na=False)
        df = df[mask].copy()
        if df.empty:
            return {}

        result = {}
        for code, group in df.groupby('代码'):
            code = str(code).zfill(6)
            result[code] = {
                'net_buy_total': float(group['龙虎榜净买额'].sum()),
                'appear_count':  int(group['上榜日'].nunique()),
                'reasons':       list(group['上榜原因'].unique()),
            }
        return result

    def get_lhb_map(self) -> Dict:
        """主入口：返回近3个交易日龙虎榜聚合数据，失败时返回空字典"""
        today = datetime.now().strftime('%Y-%m-%d')

        if self._cache.is_lhb_fresh(today):
            return self._cache.get_lhb()

        start = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        end   = datetime.now().strftime('%Y%m%d')

        try:
            df   = self._fetch_raw(start, end)
            data = self._aggregate(df)
            self._cache.set_lhb(data, today)
            logger.info(f"龙虎榜数据已更新: {len(data)} 只股票 [{start}~{end}]")
            return data
        except Exception as e:
            logger.warning(f"LHB fetch failed [{start}~{end}]: {e}")
            return {}


def apply_lhb_filter(signals: List[Dict], lhb_map: Dict) -> List[Dict]:
    """对利弗莫尔信号做龙虎榜二次过滤和标注。
    - 上榜且净买额 <= 0（含买卖相抵）：移除
    - 上榜且净买额 > 0：加 lhb_tag 标注
    - 未上榜：保留，lhb_tag 为空字符串
    """
    result = []
    for sig in signals:
        sig = {**sig}
        lhb = lhb_map.get(sig['code'])
        if lhb is not None:
            if lhb['net_buy_total'] <= 0:
                continue
            wan = lhb['net_buy_total'] / 10000
            sig['lhb_tag'] = f"[龙虎榜 ×{lhb['appear_count']} 净买+{wan:.0f}万]"
        else:
            sig['lhb_tag'] = ''
        result.append(sig)
    return result
