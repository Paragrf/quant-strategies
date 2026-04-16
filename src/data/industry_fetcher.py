# -*- coding: utf-8 -*-
"""
行业分类数据获取器
数据源: 东方财富 stock_board_industry_name_em + stock_board_industry_cons_em
缓存策略: SQLite industry_map 表，TTL 30 天
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import akshare as ak

from ._cache import StockCache

logger = logging.getLogger(__name__)


class IndustryFetcher:
    """获取并缓存「股票代码→行业名称」映射"""

    def __init__(self) -> None:
        self._cache = StockCache()

    def _fetch_sector(self, name: str) -> Dict[str, str]:
        try:
            df = ak.stock_board_industry_cons_em(symbol=name)
            if df is None or df.empty:
                return {}
            code_col = next((c for c in df.columns if '代码' in c), None)
            if not code_col:
                return {}
            return {str(row[code_col]).zfill(6): name for _, row in df.iterrows()}
        except Exception:
            return {}

    def refresh_cache(self) -> Dict[str, str]:
        try:
            sectors_df = ak.stock_board_industry_name_em()
            sector_names = sectors_df['板块名称'].tolist()
        except Exception as exc:
            logger.warning('获取行业列表失败: %s', exc)
            return {}

        logger.info('开始构建行业映射: %d 个板块', len(sector_names))
        result: Dict[str, str] = {}
        t0 = time.time()

        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(self._fetch_sector, name): name for name in sector_names}
            for future in as_completed(futures):
                result.update(future.result())

        logger.info('行业映射构建完成: %d 只股票, 耗时%.1fs', len(result), time.time() - t0)
        self._cache.set_industry_map(result)
        return result

    def load_cache(self) -> Dict[str, str]:
        return self._cache.get_industry_map() or {}

    def get_industry_map(self, force_refresh: bool = False) -> Dict[str, str]:
        if not force_refresh:
            cached = self._cache.get_industry_map()
            if cached is not None:
                return cached
        result = self.refresh_cache()
        if not result:
            logger.warning('行业API不可用，将在运行时使用名称规则降级分类')
        return result

    @staticmethod
    def classify_by_name(code: str, name: str) -> str:
        kw_map = [
            (['银行', '农行', '工行', '建行', '中行', '交行'], '银行'),
            (['保险', '人寿', '太保', '人保', '新华', '平安人寿'], '保险'),
            (['证券', '基金', '信托', '华泰', '中信', '国泰', '东方财富', '广发'], '证券'),
            (['地产', '万科', '保利', '碧桂园', '龙湖', '华润置地'], '地产'),
            (['茅台', '五粮液', '洋河', '泸州', '汾酒', '古井', '今世缘', '迎驾'], '白酒'),
            (['新能源', '宁德', '比亚迪', '亿纬锂能', '国轩'], '新能源'),
            (['医药', '药业', '制药', '生物', '医疗', '康美'], '医药'),
            (['钢铁', '宝钢', '鞍钢', '华菱'], '钢铁'),
            (['煤炭', '煤业', '神华', '陕煤'], '煤炭'),
            (['石油', '石化', '中石油', '中石化', '中海油'], '石油'),
        ]
        for keywords, sector in kw_map:
            if any(kw in name for kw in keywords):
                return sector
        return '其他'
