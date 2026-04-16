# -*- coding: utf-8 -*-
"""
股票池数据获取器
从 akshare 拉取 A股全量列表 / 沪深300成分股，存入 SQLite universe_stocks 表。
TTL 到期时自动重拉，外部无需关心 JSON 文件。
"""
import logging
import time
from datetime import date
from typing import Dict, List

import akshare as ak

from ._cache import StockCache

logger = logging.getLogger(__name__)

_RETRY = 3
_RETRY_SLEEP = 2


def _retry(fn, *args, **kwargs):
    """简单重试装饰器（同步）"""
    last_exc = None
    for attempt in range(_RETRY):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRY - 1:
                logger.warning('%s 失败（%d/%d）: %s', fn.__name__, attempt + 1, _RETRY, exc)
                time.sleep(_RETRY_SLEEP)
    raise last_exc


def _pick_col(df, *candidates: str) -> str:
    """从候选列名中找第一个存在的列"""
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f'找不到列，候选: {candidates}，实际: {list(df.columns)}')


# ---------- A 股全量 ----------

def _fetch_a_share_raw() -> List[Dict]:
    """从沪/深/北三所拉取全量 A 股列表，返回 [{code, name}, ...]"""
    stocks: Dict[str, str] = {}

    # 上交所
    try:
        for symbol in ('主板A股', '科创板'):
            df = _retry(ak.stock_info_sh_name_code, symbol=symbol)
            code_col = _pick_col(df, '证券代码', 'A股代码', '股票代码')
            name_col = _pick_col(df, '证券简称', '证券名称', 'A股简称', '股票名称')
            for _, row in df.iterrows():
                code = str(row[code_col]).zfill(6)
                stocks[code] = str(row[name_col])
        logger.info('上交所: %d 只', len(stocks))
    except Exception as exc:
        logger.warning('上交所拉取失败: %s', exc)

    # 深交所
    try:
        before = len(stocks)
        df = _retry(ak.stock_info_sz_name_code, symbol='A股列表')
        code_col = _pick_col(df, 'A股代码', '证券代码', '股票代码')
        name_col = _pick_col(df, 'A股简称', '证券名称', '股票名称')
        for _, row in df.iterrows():
            code = str(row[code_col]).zfill(6)
            stocks[code] = str(row[name_col])
        logger.info('深交所: +%d 只', len(stocks) - before)
    except Exception as exc:
        logger.warning('深交所拉取失败: %s', exc)

    # 北交所
    try:
        before = len(stocks)
        df = _retry(ak.stock_info_bj_name_code)
        code_col = _pick_col(df, '证券代码', 'A股代码', '股票代码')
        name_col = _pick_col(df, '证券简称', '证券名称', 'A股简称', '股票名称')
        for _, row in df.iterrows():
            code = str(row[code_col]).zfill(6)
            stocks[code] = str(row[name_col])
        logger.info('北交所: +%d 只', len(stocks) - before)
    except Exception as exc:
        logger.warning('北交所拉取失败: %s', exc)

    return [{'code': c, 'name': n} for c, n in stocks.items()]


def get_a_share(cache: StockCache) -> List[Dict]:
    """返回 A 股全量列表 [{code, name}]，优先走 SQLite 缓存。"""
    cached = cache.get_stock_list('a_share')
    if cached is not None:
        return cached

    logger.info('A股列表缓存过期，从 akshare 重拉...')
    stocks = _fetch_a_share_raw()
    if stocks:
        cache.set_stock_list(
            'a_share', stocks,
            update_date=date.today().isoformat(),
            data_source='akshare.stock_info_sh/sz/bj_name_code',
        )
        logger.info('A股列表已更新: %d 只', len(stocks))
    return stocks


# ---------- 沪深 300 ----------

def _fetch_csi300_raw() -> List[Dict]:
    df = _retry(ak.index_stock_cons, symbol='000300')
    code_col = _pick_col(df, '品种代码', '成分券代码', '股票代码')
    name_col = _pick_col(df, '品种名称', '成分券名称', '股票名称')
    return [
        {'code': str(row[code_col]).zfill(6), 'name': str(row[name_col])}
        for _, row in df.iterrows()
    ]


def get_csi300(cache: StockCache) -> List[Dict]:
    """返回沪深300成分股 [{code, name}]，优先走 SQLite 缓存。"""
    cached = cache.get_stock_list('csi300')
    if cached is not None:
        return cached

    logger.info('沪深300缓存过期，从 akshare 重拉...')
    stocks = _fetch_csi300_raw()
    if stocks:
        cache.set_stock_list(
            'csi300', stocks,
            update_date=date.today().isoformat(),
            data_source='akshare.index_stock_cons(000300)',
        )
        logger.info('沪深300已更新: %d 只', len(stocks))
    return stocks
