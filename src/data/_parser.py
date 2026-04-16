import json
import logging
from typing import Callable, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _safe_float(parts: list, idx: int) -> Optional[float]:
    try:
        v = parts[idx] if len(parts) > idx else ''
        return float(v) if v else None
    except (ValueError, TypeError):
        return None


def parse_gtimg_stock(
    content: str,
    stock_code: str,
    manual_dividend_fn: Optional[Callable[[str], Optional[float]]] = None,
) -> Dict:
    try:
        if not content or 'v_' not in content:
            return {}
        parts = content.split('"')
        if len(parts) < 2:
            return {}
        data_parts = parts[1].split('~')
        if len(data_parts) <= 35:
            return {}

        name       = data_parts[1]
        price      = _safe_float(data_parts, 3) or 0.0
        prev_close = _safe_float(data_parts, 4) or 0.0
        change_pct = _safe_float(data_parts, 32) or 0.0
        volume     = int(float(data_parts[6])) if data_parts[6] else 0
        turnover   = int(float(data_parts[7])) if data_parts[7] else 0
        market_cap   = _safe_float(data_parts, 23)
        total_shares = _safe_float(data_parts, 25)

        turnover_rate = None
        for idx in (56, 27):
            v = _safe_float(data_parts, idx)
            if v is not None:
                turnover_rate = v
                break

        pe_ratio = None
        for idx in (39, 22, 15, 14):
            v = _safe_float(data_parts, idx)
            if v is not None and 0 < v < 1000:
                pe_ratio = v
                break

        pb_ratio = None
        for idx in (46, 16):
            v = _safe_float(data_parts, idx)
            if v is not None and 0 < v < 100:
                pb_ratio = v
                break

        dividend_yield: Optional[float] = None
        if manual_dividend_fn is not None:
            dividend_yield = manual_dividend_fn(stock_code)
        if dividend_yield is None:
            dv = _safe_float(data_parts, 53)
            if dv and dv > 0 and price > 0:
                per_share = dv / 10
                dy = (per_share / price) * 100
                if 0 < dy <= 20:
                    dividend_yield = dy

        roe: Optional[float] = None
        if pb_ratio is not None and pe_ratio is not None and pe_ratio > 0:
            r = (pb_ratio / pe_ratio) * 100
            if -50 <= r <= 50:
                roe = r

        profit_growth: Optional[float] = None
        if roe and dividend_yield is not None and dividend_yield > 0 and roe > 0:
            payout = min(dividend_yield / roe, 0.9)
            profit_growth = roe * (1 - payout)

        peg: Optional[float] = None
        if pe_ratio:
            assumed_growth = 20 if (pb_ratio and pb_ratio < 1) else (10 if (pb_ratio and pb_ratio > 5) else 15)
            peg = pe_ratio / assumed_growth

        return {
            'code': stock_code,
            'name': name,
            'price': price,
            'prev_close': prev_close,
            'change_pct': change_pct,
            'volume': volume,
            'turnover': turnover,
            'market_cap': market_cap,
            'total_shares': total_shares,
            'turnover_rate': turnover_rate,
            'pe_ratio': pe_ratio,
            'pb_ratio': pb_ratio,
            'dividend_yield': dividend_yield,
            'peg': peg,
            'roe': roe,
            'profit_growth': profit_growth,
            'financial_health_score': calculate_financial_health(pb_ratio, dividend_yield, pe_ratio, turnover_rate),
            'debt_ratio': None,
            'current_ratio': None,
            'gross_margin': None,
        }
    except Exception as exc:
        logger.debug('解析股票 %s 数据失败: %s', stock_code, exc)
        return {}


def parse_kline(content: str, stock_code: str, hist_days: int) -> pd.DataFrame:
    try:
        if not content or 'kline_dayqfq=' not in content:
            return pd.DataFrame()
        json_str = content.replace('kline_dayqfq=', '')
        data_json = json.loads(json_str)
        prefix = 'sh' if stock_code.startswith('6') else 'sz'
        symbol = f'{prefix}{stock_code}'
        klines = data_json.get('data', {}).get(symbol, {}).get('qfqday', [])
        if not klines:
            return pd.DataFrame()
        rows = [
            {
                'date':   kl[0],
                'open':   float(kl[1]),
                'close':  float(kl[2]),
                'high':   float(kl[3]),
                'low':    float(kl[4]),
                'volume': float(kl[5]) if len(kl) > 5 else 0.0,
            }
            for kl in klines
        ]
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        if len(df) > hist_days:
            df = df.tail(hist_days).reset_index(drop=True)
        return df
    except Exception as exc:
        logger.debug('解析 %s K线数据失败: %s', stock_code, exc)
        return pd.DataFrame()


def calculate_financial_health(
    pb: Optional[float],
    div_yield: Optional[float],
    pe: Optional[float],
    turnover: Optional[float],
) -> int:
    score = 50
    try:
        if pb is not None:
            if pb < 1:       score += 20
            elif pb < 2:     score += 10
            elif pb > 10:    score -= 20
            elif pb > 5:     score -= 10
        if div_yield is not None:
            if div_yield > 5:    score += 15
            elif div_yield > 3:  score += 10
            elif div_yield > 2:  score += 5
            elif div_yield < 1:  score -= 5
        if pe is not None:
            if 10 < pe < 20:     score += 10
            elif 20 <= pe < 30:  score += 5
            elif pe >= 50:       score -= 10
        if turnover is not None:
            if 1 < turnover < 5:  score += 5
            elif turnover > 20:   score -= 5
    except Exception:
        pass
    return max(0, min(100, score))


def calculate_momentum(price_data: pd.DataFrame, days: int = 20) -> float:
    if len(price_data) < days:
        return 0.0
    try:
        recent = price_data['close'].tail(days)
        return (recent.iloc[-1] / recent.iloc[0] - 1) * 100
    except Exception as exc:
        logger.debug('计算动量失败: %s', exc)
        return 0.0
