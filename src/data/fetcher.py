# src/data/fetcher.py
import asyncio
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.dividend_override import get_manual_dividend_yield

from ._cache import StockCache
from ._http import fetch_with_retry, get_random_ua
from ._parser import calculate_momentum as _calc_momentum, parse_gtimg_stock, parse_kline
from .industry_fetcher import IndustryFetcher
from .universe_fetcher import get_csi300

logger = logging.getLogger(__name__)


class AsyncStockDataFetcher:
    """异步股票数据获取器。"""

    def __init__(self, max_concurrent: int = 20) -> None:
        self.max_concurrent = max_concurrent
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.failed_stocks: List[str] = []
        self._cache = StockCache()
        self._industry_map: Optional[Dict[str, str]] = None

    def _get_industry_map(self) -> Dict[str, str]:
        if self._industry_map is None:
            fetcher = IndustryFetcher()
            self._industry_map = fetcher.get_industry_map()
        return self._industry_map

    def _resolve_industry(self, code: str, name: str) -> str:
        industry_map = self._get_industry_map()
        if industry_map:
            return industry_map.get(code, IndustryFetcher.classify_by_name(code, name))
        return IndustryFetcher.classify_by_name(code, name)

    async def _get_stock_data(
        self, session: aiohttp.ClientSession, stock_code: str
    ) -> Dict:
        """获取单只股票全量数据（实时 + 基本面），优先读缓存。"""
        cached = self._cache.get_stock(stock_code)
        if cached:
            return cached

        async with self.semaphore:
            prefix = 'sh' if stock_code.startswith('6') else 'sz'
            url = f'https://qt.gtimg.cn/q={prefix}{stock_code}'
            content = await fetch_with_retry(session, url, max_retries=3, timeout=10)

        result = parse_gtimg_stock(
            content or '',
            stock_code,
            manual_dividend_fn=get_manual_dividend_yield,
        )
        if result:
            self._cache.set_stock(stock_code, result)
        return result

    async def get_stock_historical_data(
        self, session: aiohttp.ClientSession, stock_code: str, hist_days: int = 90
    ) -> pd.DataFrame:
        cache_key = f'{stock_code}_{hist_days}'
        cached = self._cache.get_hist(cache_key)
        if cached is not None:
            return cached

        async with self.semaphore:
            prefix = 'sh' if stock_code.startswith('6') else 'sz'
            symbol = f'{prefix}{stock_code}'
            actual_days = hist_days * 2
            url = (
                f'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
                f'?param={symbol},day,,,{actual_days},qfq&_var=kline_dayqfq'
            )
            content = await fetch_with_retry(session, url, max_retries=3, timeout=15)

        df = parse_kline(content or '', stock_code, hist_days)
        if not df.empty:
            self._cache.set_hist(cache_key, df)
        return df

    async def batch_get_stock_data(
        self,
        stock_codes: List[str],
        calculate_momentum: bool = True,
        include_fundamental: bool = True,  # 保留参数，保持与原版接口兼容
    ) -> List[Dict]:
        """批量异步获取股票数据。接口与原版保持一致。"""
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        stock_codes = list(set(stock_codes))
        logger.info('开始批量获取 %d 只股票数据 (最大并发: %d)', len(stock_codes), self.max_concurrent)
        start_time = time.time()

        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            limit_per_host=self.max_concurrent,
            ttl_dns_cache=300,
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=15)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            logger.info('步骤1: 批量获取股票数据...')
            tasks = [self._get_stock_data(session, code) for code in stock_codes]
            results = await asyncio.gather(*tasks)
            valid_stocks = [d for d in results if d and d.get('code')]
            logger.info('成功获取 %d/%d 只股票数据', len(valid_stocks), len(stock_codes))

            if not valid_stocks:
                return []

            if calculate_momentum:
                logger.info('步骤2: 批量获取历史数据并计算动量...')
                hist_tasks = [
                    self.get_stock_historical_data(session, s['code'], hist_days=90)
                    for s in valid_stocks
                ]
                hist_results = await asyncio.gather(*hist_tasks)
                momentum_ok = 0
                for stock, hist_df in zip(valid_stocks, hist_results):
                    if isinstance(hist_df, pd.DataFrame) and not hist_df.empty and len(hist_df) >= 20:
                        stock['momentum_20d'] = _calc_momentum(hist_df, days=20)
                        momentum_ok += 1
                    else:
                        stock['momentum_20d'] = 0
                logger.info('动量计算成功: %d/%d', momentum_ok, len(valid_stocks))
            else:
                for s in valid_stocks:
                    s['momentum_20d'] = 0

        logger.info('步骤3: 查询行业信息...')
        for s in valid_stocks:
            s['industry'] = self._resolve_industry(s['code'], s.get('name', ''))

        elapsed = time.time() - start_time
        logger.info('批量获取完成! 用时: %.2fs, 速度: %.1f只/秒', elapsed, len(valid_stocks) / elapsed)
        return valid_stocks

    async def get_market_overview_async(self) -> Dict:
        """异步获取市场概况，优先读 SQLite 缓存。"""
        cached = self._cache.get_market_overview()
        if cached:
            logger.info('使用缓存的市场概况数据')
            return cached

        logger.info('正在获取市场概况数据...')
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=10)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                headers = {'User-Agent': get_random_ua(), 'Referer': 'https://gu.qq.com/'}

                url = 'https://qt.gtimg.cn/q=sh000001,sz399001,sz399006'
                index_data = []
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            content = await resp.text()
                            for line in content.strip().split(';'):
                                if 'v_' in line and '~' in line:
                                    parts = line.split('"')[1].split('~')
                                    if len(parts) > 32:
                                        index_data.append({
                                            'name': parts[1],
                                            'change_pct': float(parts[32]) if parts[32] else 0,
                                            'price': float(parts[3]) if parts[3] else 0,
                                        })
                except Exception as exc:
                    logger.warning('获取指数数据失败: %s', exc)

                avg_change = sum(d['change_pct'] for d in index_data[:3]) / 3 if index_data else 0

                rising = falling = flat = 0
                csi300_codes = [s['code'] for s in get_csi300(self._cache)]

                if csi300_codes:
                    for i in range(0, len(csi300_codes), 100):
                        batch = csi300_codes[i:i + 100]
                        symbols = [f"{'sh' if c.startswith('6') else 'sz'}{c}" for c in batch]
                        batch_url = f"https://qt.gtimg.cn/q={','.join(symbols)}"
                        try:
                            async with session.get(batch_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                                if resp.status == 200:
                                    for line in (await resp.text()).strip().split(';'):
                                        if 'v_' in line and '~' in line:
                                            try:
                                                parts = line.split('"')[1].split('~')
                                                if len(parts) > 32:
                                                    cp = float(parts[32]) if parts[32] else 0
                                                    if cp > 0:   rising  += 1
                                                    elif cp < 0: falling += 1
                                                    else:        flat    += 1
                                            except Exception:
                                                continue
                        except Exception as exc:
                            logger.debug('批次查询失败: %s', exc)
                        await asyncio.sleep(0.3)

                total = rising + falling + flat
                if total == 0:
                    logger.warning('沪深300数据文件不存在或无有效数据，跳过市场统计缓存')
                    return {
                        'total_stocks': 0, 'rising_stocks': 0, 'falling_stocks': 0,
                        'flat_stocks': 0, 'rising_ratio': 50.0, 'avg_change_pct': avg_change,
                        'update_time': datetime.now().isoformat(), 'data_source': '指数数据(无成分股统计)',
                        'indices': index_data, 'success_count': 0,
                    }
                rising_ratio = rising / total * 100
                overview = {
                    'total_stocks': total,
                    'rising_stocks': rising,
                    'falling_stocks': falling,
                    'flat_stocks': flat,
                    'rising_ratio': rising_ratio,
                    'avg_change_pct': avg_change,
                    'update_time': datetime.now().isoformat(),
                    'data_source': '腾讯财经实时数据(沪深300)',
                    'indices': index_data,
                    'success_count': total,
                }
                self._cache.set_market_overview(overview)
                logger.info('市场概况获取成功: 上涨%d, 下跌%d, 比例%.2f%%', rising, falling, rising_ratio)
                return overview

        except Exception as exc:
            logger.error('获取市场概况失败: %s', exc)
            return {
                'total_stocks': 300, 'rising_stocks': 135, 'falling_stocks': 105,
                'flat_stocks': 60, 'rising_ratio': 45.0, 'avg_change_pct': 0.2,
                'update_time': datetime.now().isoformat(), 'data_source': '错误兜底',
                'error': str(exc),
            }


def batch_get_stock_data_sync(
    stock_codes: List[str],
    calculate_momentum: bool = True,
    include_fundamental: bool = True,
    max_concurrent: int = 20,
) -> List[Dict]:
    fetcher = AsyncStockDataFetcher(max_concurrent=max_concurrent)
    return asyncio.run(
        fetcher.batch_get_stock_data(stock_codes, calculate_momentum, include_fundamental)
    )


def get_market_overview_sync() -> Dict:
    fetcher = AsyncStockDataFetcher()
    return asyncio.run(fetcher.get_market_overview_async())
