# quant-strategies

A collection of quantitative stock screening strategies for the A-share market, built with async data fetching and a SQLite-backed cache layer.

---

## Strategies

### 1. Livermore Breakout (`run_livermore.py`)

Scans the A-share universe for Jesse Livermore-style breakout signals.

**Signal types:**
- **Pivotal Point Breakout** — price breaks above a consolidation box on high volume (≥2× avg)
- **Continuation Point** — pullback after a strong trend, then breaks out of a low-volume consolidation
- **Top Test** — price quietly tests a historical high on low volume, then breaks out

**Scoring:** signal strength + trend score (MA20/MA60 alignment) + volume ratio + breakout magnitude + sector resonance bonus

**Filters:** market cap ≥ 3B, daily turnover ≥ 30M, skip ST stocks, skip stale data (halted stocks)

---

### 2. Dividend Dip (`run_dividend_dip.py`)

Scans dividend index constituents (CSI Dividend 000922 + SSE Dividend 000015) for high-yield stocks trading below their 120-day moving average.

**Trigger:** current price ≤ MA120 × (1 − threshold), default threshold = 10%

**Output:** discount to MA120, dividend yield, sorted by largest discount first

---

### 3. MA Reversal (`run_ma_reversal.py`)

Scans CSI 300 constituents for mean-reversion setups near key long-term moving averages, designed to catch potential trend reversals before they happen.

**5-step filter (all must pass):**

| Step | Condition | Default |
|------|-----------|---------|
| 1 | Skip ST/\*ST stocks | — |
| 2 | Pulled back ≥ 10% from 60-day high | `min_drawdown=0.10` |
| 3 | Current price within ±3% of MA120 or MA250 | `ma_tolerance=0.03` |
| 4 | Recent 7-day avg volume < 80% of 60-day avg | `vol_ratio_threshold=0.80` |
| 5 | Recent 10-day return std < 80% of 60-day std | `vol_narrow_threshold=0.80` |

**Scoring (max 105):**

```
signal_score = (1 - |MA deviation| / tolerance) × 40   # proximity to MA
             + (1 - vol_ratio) × 30                     # volume drying up
             + (1 - volatility_ratio) × 30              # volatility narrowing
             + 5 (if both MA120 and MA250 triggered)     # dual-MA bonus
```

---

## Project Structure

```
quant_strategies/
├── src/
│   ├── analysis/
│   │   ├── livermore_filter.py     # Livermore breakout logic
│   │   ├── dividend_filter.py      # Dividend dip logic
│   │   └── ma_reversal_filter.py   # MA reversal logic
│   └── data/
│       ├── fetcher.py              # Async stock data fetcher
│       ├── universe_fetcher.py     # A-share / CSI300 universe loader
│       ├── industry_fetcher.py     # Industry classification
│       ├── lhb_fetcher.py          # Dragon-Tiger Board (LHB) data
│       ├── _cache.py               # SQLite cache layer
│       ├── _http.py                # HTTP utilities with retry
│       └── _parser.py              # Response parsers
├── config/
│   ├── config.py                   # Global config
│   └── dividend_override.py        # Manual dividend yield overrides
├── tests/
│   ├── data/                       # Cache and fetcher tests
│   └── analysis/                   # Strategy unit tests
├── run_livermore.py                 # Entry point: Livermore scan
├── run_dividend_dip.py              # Entry point: Dividend dip scan
└── run_ma_reversal.py              # Entry point: MA reversal scan
```

---

## Data Sources

- **Price & realtime data:** Tencent Finance (`qt.gtimg.cn`)
- **Historical K-line:** ifzq (`web.ifzq.gtimg.cn`)
- **Index constituents / industry:** akshare
- **Dragon-Tiger Board:** akshare

All data is cached in SQLite (`./cache/stock_cache.db`) to minimize repeated network requests.

---

## Installation

```bash
pip install -r requirements.txt
```

**Requirements:** Python 3.10+

---

## Usage

```bash
# Livermore breakout scan (full A-share universe)
python run_livermore.py

# Dividend dip scan (dividend index constituents)
python run_dividend_dip.py

# MA reversal scan (CSI 300)
python run_ma_reversal.py
```

Each script prints a formatted results table to stdout and saves results to SQLite.

---

## Running Tests

```bash
python -m pytest -v
```
