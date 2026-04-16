import logging
import os
import sqlite3
import time
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

TTL_STOCK    = 3600         # 1 小时
TTL_HIST     = 3600         # 1 小时
TTL_OVERVIEW = 300          # 5 分钟
TTL_UNIVERSE = 86400 * 30   # 30 天

_DDL = """
CREATE TABLE IF NOT EXISTS stock_cache (
    code       TEXT PRIMARY KEY,
    data_json  TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS hist_cache (
    cache_key  TEXT PRIMARY KEY,
    data_json  TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS market_overview_cache (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    data_json  TEXT NOT NULL,
    updated_at REAL NOT NULL
);

-- 股票池成分股（a_share / csi300 / dividend_universe / ...）
CREATE TABLE IF NOT EXISTS universe_stocks (
    list_key   TEXT NOT NULL,
    code       TEXT NOT NULL,
    name       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (list_key, code)
);
-- 股票池元数据（更新时间、来源）
CREATE TABLE IF NOT EXISTS universe_meta (
    list_key    TEXT PRIMARY KEY,
    update_date TEXT NOT NULL DEFAULT '',
    data_source TEXT NOT NULL DEFAULT '',
    updated_at  REAL NOT NULL
);
-- 行业映射（code → industry）
CREATE TABLE IF NOT EXISTS industry_map (
    code       TEXT PRIMARY KEY,
    industry   TEXT NOT NULL
);
-- 龙虎榜聚合数据（当日有效）
CREATE TABLE IF NOT EXISTS lhb_cache (
    code          TEXT PRIMARY KEY,
    net_buy_total REAL    NOT NULL,
    appear_count  INTEGER NOT NULL,
    reasons       TEXT    NOT NULL,  -- JSON 数组
    cache_date    TEXT    NOT NULL   -- 'YYYY-MM-DD'
);
-- 利弗莫尔策略扫描结果
CREATE TABLE IF NOT EXISTS livermore_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date       TEXT    NOT NULL,
    code            TEXT    NOT NULL,
    name            TEXT    NOT NULL DEFAULT '',
    signal_types    TEXT    NOT NULL,  -- JSON 数组
    signal_details  TEXT    NOT NULL,  -- JSON 对象
    signal_strength INTEGER NOT NULL,
    trend_score     REAL    NOT NULL DEFAULT 0,
    signal_score    REAL    NOT NULL,
    signal_date     TEXT    NOT NULL,
    limit_tag       TEXT    NOT NULL DEFAULT '',
    lhb_tag         TEXT    NOT NULL DEFAULT '',
    sector_tag      TEXT    NOT NULL DEFAULT '',
    sector_hot_tag  TEXT    NOT NULL DEFAULT '',
    UNIQUE (scan_date, code)
);
-- 红利低吸策略扫描结果
CREATE TABLE IF NOT EXISTS dividend_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date      TEXT NOT NULL,
    code           TEXT NOT NULL,
    name           TEXT NOT NULL DEFAULT '',
    current_price  REAL NOT NULL,
    ma120          REAL NOT NULL,
    discount_pct   REAL NOT NULL,
    dividend_yield REAL NOT NULL,
    signal_date    TEXT NOT NULL,
    UNIQUE (scan_date, code)
);
-- 均线支撑反转策略扫描结果
CREATE TABLE IF NOT EXISTS ma_reversal_results (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date         TEXT    NOT NULL,
    code              TEXT    NOT NULL,
    name              TEXT    NOT NULL DEFAULT '',
    signal_score      REAL    NOT NULL,
    triggered_ma      TEXT    NOT NULL DEFAULT '',
    ma_proximity_pct  REAL    NOT NULL DEFAULT 0,
    drawdown_pct      REAL    NOT NULL DEFAULT 0,
    vol_ratio         REAL    NOT NULL DEFAULT 0,
    vol_narrow_ratio  REAL    NOT NULL DEFAULT 0,
    UNIQUE (scan_date, code)
);
"""

import json


class StockCache:
    def __init__(self, db_path: str = './cache/stock_cache.db') -> None:
        self._db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(_DDL)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _is_fresh(self, conn: sqlite3.Connection, list_key: str, ttl: float) -> bool:
        row = conn.execute(
            'SELECT updated_at FROM universe_meta WHERE list_key = ?', (list_key,)
        ).fetchone()
        return bool(row and time.time() - row['updated_at'] < ttl)

    # ---------- stock ----------

    def get_stock(self, code: str) -> Optional[Dict]:
        with self._conn() as conn:
            row = conn.execute(
                'SELECT data_json, updated_at FROM stock_cache WHERE code = ?', (code,)
            ).fetchone()
        if row and time.time() - row['updated_at'] < TTL_STOCK:
            return json.loads(row['data_json'])
        return None

    def set_stock(self, code: str, data: Dict) -> None:
        with self._conn() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO stock_cache (code, data_json, updated_at) VALUES (?, ?, ?)',
                (code, json.dumps(data, ensure_ascii=False), time.time()),
            )

    # ---------- hist ----------

    def get_hist(self, cache_key: str) -> Optional[pd.DataFrame]:
        with self._conn() as conn:
            row = conn.execute(
                'SELECT data_json, updated_at FROM hist_cache WHERE cache_key = ?', (cache_key,)
            ).fetchone()
        if row and time.time() - row['updated_at'] < TTL_HIST:
            import io
            df = pd.read_json(io.StringIO(row['data_json']), orient='records')
            df['date'] = pd.to_datetime(df['date'])
            return df
        return None

    def set_hist(self, cache_key: str, df: pd.DataFrame) -> None:
        df_copy = df.copy()
        df_copy['date'] = df_copy['date'].dt.strftime('%Y-%m-%d')
        with self._conn() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO hist_cache (cache_key, data_json, updated_at) VALUES (?, ?, ?)',
                (cache_key, df_copy.to_json(orient='records'), time.time()),
            )

    # ---------- market overview ----------

    def get_market_overview(self) -> Optional[Dict]:
        with self._conn() as conn:
            row = conn.execute(
                'SELECT data_json, updated_at FROM market_overview_cache WHERE id = 1'
            ).fetchone()
        if row and time.time() - row['updated_at'] < TTL_OVERVIEW:
            return json.loads(row['data_json'])
        return None

    def set_market_overview(self, data: Dict) -> None:
        safe = {
            k: v if isinstance(v, (int, float, str, bool, list, dict, type(None))) else str(v)
            for k, v in data.items()
        }
        with self._conn() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO market_overview_cache (id, data_json, updated_at) VALUES (1, ?, ?)',
                (json.dumps(safe, ensure_ascii=False), time.time()),
            )

    # ---------- universe stocks（股票池成分股） ----------

    def get_stock_list(self, key: str) -> Optional[List[Dict[str, str]]]:
        """返回 [{code, name}, ...] 若缓存新鲜，否则 None。"""
        with self._conn() as conn:
            if not self._is_fresh(conn, key, TTL_UNIVERSE):
                return None
            rows = conn.execute(
                'SELECT code, name FROM universe_stocks WHERE list_key = ? ORDER BY code',
                (key,),
            ).fetchall()
        return [{'code': r['code'], 'name': r['name']} for r in rows]

    def set_stock_list(
        self,
        key: str,
        stocks: List[Dict[str, str]],
        update_date: str = '',
        data_source: str = '',
    ) -> None:
        """原子替换整个股票池，同时更新元数据。"""
        with self._conn() as conn:
            conn.execute('DELETE FROM universe_stocks WHERE list_key = ?', (key,))
            conn.executemany(
                'INSERT INTO universe_stocks (list_key, code, name) VALUES (?, ?, ?)',
                [(key, s['code'], s.get('name', '')) for s in stocks],
            )
            conn.execute(
                '''INSERT OR REPLACE INTO universe_meta (list_key, update_date, data_source, updated_at)
                   VALUES (?, ?, ?, ?)''',
                (key, update_date, data_source, time.time()),
            )

    # ---------- industry map（行业映射） ----------

    def get_industry_map(self) -> Optional[Dict[str, str]]:
        """返回 {code: industry} 若缓存新鲜，否则 None。"""
        with self._conn() as conn:
            if not self._is_fresh(conn, 'industry_map', TTL_UNIVERSE):
                return None
            rows = conn.execute('SELECT code, industry FROM industry_map').fetchall()
        return {r['code']: r['industry'] for r in rows}

    def set_industry_map(self, data: Dict[str, str]) -> None:
        """原子替换全量行业映射。"""
        with self._conn() as conn:
            conn.execute('DELETE FROM industry_map')
            conn.executemany(
                'INSERT INTO industry_map (code, industry) VALUES (?, ?)',
                data.items(),
            )
            conn.execute(
                '''INSERT OR REPLACE INTO universe_meta (list_key, updated_at)
                   VALUES ('industry_map', ?)''',
                (time.time(),),
            )

    # ---------- lhb cache（龙虎榜，当日有效） ----------

    def is_lhb_fresh(self, today: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                'SELECT cache_date FROM lhb_cache LIMIT 1'
            ).fetchone()
        return bool(row and row['cache_date'] == today)

    def get_lhb(self) -> Dict[str, Dict]:
        """返回 {code: {net_buy_total, appear_count, reasons}}，不检查新鲜度。"""
        with self._conn() as conn:
            rows = conn.execute(
                'SELECT code, net_buy_total, appear_count, reasons FROM lhb_cache'
            ).fetchall()
        return {
            r['code']: {
                'net_buy_total': r['net_buy_total'],
                'appear_count':  r['appear_count'],
                'reasons':       json.loads(r['reasons']),
            }
            for r in rows
        }

    def set_lhb(self, data: Dict[str, Dict], today: str) -> None:
        """原子替换全部龙虎榜数据。"""
        with self._conn() as conn:
            conn.execute('DELETE FROM lhb_cache')
            conn.executemany(
                '''INSERT INTO lhb_cache (code, net_buy_total, appear_count, reasons, cache_date)
                   VALUES (?, ?, ?, ?, ?)''',
                [
                    (code, v['net_buy_total'], v['appear_count'],
                     json.dumps(v['reasons'], ensure_ascii=False), today)
                    for code, v in data.items()
                ],
            )

    # ---------- strategy results（策略扫描结果持久化） ----------

    def save_livermore_results(self, signals: List[Dict], scan_date: str) -> int:
        """保存利弗莫尔扫描结果，同一天同一股票重复扫描则覆盖。返回写入行数。"""
        with self._conn() as conn:
            conn.executemany(
                '''INSERT OR REPLACE INTO livermore_results
                   (scan_date, code, name, signal_types, signal_details,
                    signal_strength, trend_score, signal_score, signal_date,
                    limit_tag, lhb_tag, sector_tag, sector_hot_tag)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [
                    (
                        scan_date,
                        s['code'], s.get('name', ''),
                        json.dumps(s['signal_types'], ensure_ascii=False),
                        json.dumps(s['signal_details'], ensure_ascii=False),
                        s['signal_strength'],
                        s.get('trend_score', 0),
                        s['signal_score'],
                        s['signal_date'],
                        s.get('limit_tag', ''),
                        s.get('lhb_tag', ''),
                        s.get('sector_tag', ''),
                        s.get('sector_hot_tag', ''),
                    )
                    for s in signals
                ],
            )
        return len(signals)

    def save_dividend_results(self, hits: List[Dict], scan_date: str) -> int:
        """保存红利低吸扫描结果，同一天同一股票重复扫描则覆盖。返回写入行数。"""
        with self._conn() as conn:
            conn.executemany(
                '''INSERT OR REPLACE INTO dividend_results
                   (scan_date, code, name, current_price, ma120,
                    discount_pct, dividend_yield, signal_date)
                   VALUES (?,?,?,?,?,?,?,?)''',
                [
                    (
                        scan_date,
                        h['code'], h.get('name', ''),
                        h['current_price'], h['ma120'],
                        h['discount_pct'], h['dividend_yield'],
                        h['signal_date'],
                    )
                    for h in hits
                ],
            )
        return len(hits)

    def save_ma_reversal_results(self, signals: List[Dict], scan_date: str) -> int:
        """保存均线反转扫描结果，同一天同一股票重复扫描则覆盖。返回写入行数。"""
        with self._conn() as conn:
            conn.executemany(
                '''INSERT OR REPLACE INTO ma_reversal_results
                   (scan_date, code, name, signal_score, triggered_ma,
                    ma_proximity_pct, drawdown_pct, vol_ratio, vol_narrow_ratio)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                [
                    (
                        scan_date,
                        s['code'], s.get('name', ''),
                        s['signal_score'],
                        s.get('triggered_ma', ''),
                        s.get('ma_proximity_pct', 0),
                        s.get('drawdown_pct', 0),
                        s.get('vol_ratio', 0),
                        s.get('vol_narrow_ratio', 0),
                    )
                    for s in signals
                ],
            )
        return len(signals)
