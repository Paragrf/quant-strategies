# MA Trend Pullback 策略优化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 MATrendPullbackFilter 增加回踩深度限制和缩量确认过滤，并新增快速回测脚本验证优化效果。

**Architecture:** 在 `_analyze_stock` 的 Step 4 之后插入两个新过滤步骤（深度限制 → 成交量缩量），不改变现有步骤顺序。回测脚本独立存在，用滚动窗口回放历史数据，复用 Filter 的 `_analyze_stock` 方法。

**Tech Stack:** Python 3.10+, pandas, numpy, aiohttp, pytest, argparse

---

## File Map

| 动作 | 文件 | 职责 |
|------|------|------|
| Modify | `src/analysis/ma_trend_pullback_filter.py` | 新增 `proximity_min` 和 `volume_ratio_max` 参数及过滤逻辑 |
| Create | `tests/analysis/test_ma_trend_pullback_filter.py` | 新两个过滤项的单元测试 |
| Create | `run_backtest.py` | 固定持有天数回测脚本，CLI 参数控制 |

---

## Task 1: 为 proximity_min 写失败测试

**Files:**
- Create: `tests/analysis/test_ma_trend_pullback_filter.py`

- [ ] **Step 1: 新建测试文件，写辅助函数和 proximity_min 失败测试**

```python
# tests/analysis/test_ma_trend_pullback_filter.py
import numpy as np
import pandas as pd
import pytest
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter


def _make_uptrend_hist(n: int = 400, pullback_pct: float = -0.03) -> pd.DataFrame:
    """
    构造能通过斜率和穿越过滤的合成数据：
    - 价格整体线性上涨（MA 有正斜率）
    - 近 cross_window 日内有多次穿越
    - 当前价在均线下方 pullback_pct（负值）
    - 成交量回踩段缩量（vol_ratio ≈ 0.5）
    """
    dates = pd.date_range('2023-01-01', periods=n, freq='B')
    # 线性上涨基底：从 8 涨到 12（MA120 约 10，MA250 约 9.5）
    base = np.linspace(8.0, 12.0, n)

    # 近 cross_window(60)+1 = 61 日，制造 3 次穿越 MA120
    # MA120 在末尾大约是 base[-1] * (120 平均 / 最末)，简化：直接用 base 均值控制
    # 用 sin 波制造穿越
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone

    # 让最后一天价格低于 MA120（pullback_pct）
    # MA120 末尾 ≈ mean(base[-120:]) ≈ (base[-120] + base[-1]) / 2
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * (1 + pullback_pct)

    # 缩量：回踩近 5 日均量 = 50，前 20 日均量 = 100
    volumes = np.ones(n) * 100.0
    volumes[-5:] = 50.0

    return pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })


def test_proximity_min_filters_deep_pullback():
    """回踩超过 proximity_min(-3%) 应被过滤返回 None。"""
    f = MATrendPullbackFilter(proximity_min=-0.03)
    hist = _make_uptrend_hist(pullback_pct=-0.06)   # 跌 6%，超过限制
    result = f._analyze_stock(hist)
    assert result is None


def test_proximity_min_passes_shallow_pullback():
    """回踩在 proximity_min(-3%) 范围内应通过。"""
    f = MATrendPullbackFilter(proximity_min=-0.03)
    hist = _make_uptrend_hist(pullback_pct=-0.02)   # 跌 2%，在限制内
    result = f._analyze_stock(hist)
    # 注意：此数据不一定通过所有过滤（斜率/穿越），仅验证 proximity_min 不是拦截方
    # 用 proximity_min 极小值确保只测深度限制
    f2 = MATrendPullbackFilter(proximity_min=-0.03, min_slope_pct=0.0, min_cross_count=0)
    result2 = f2._analyze_stock(hist)
    assert result2 is not None
    assert result2['proximity_pct'] > -3.0
```

- [ ] **Step 2: 运行测试，确认失败（函数不存在）**

```bash
cd /path/to/worktree && python -m pytest tests/analysis/test_ma_trend_pullback_filter.py -v 2>&1 | head -30
```

期望：`ImportError` 或 `TypeError: __init__() got an unexpected keyword argument 'proximity_min'`

---

## Task 2: 实现 proximity_min 过滤

**Files:**
- Modify: `src/analysis/ma_trend_pullback_filter.py`

- [ ] **Step 1: 在 `__init__` 增加 `proximity_min` 参数**

在 `hist_days: int = 400,` 之后加一行：

```python
        proximity_min: float = -0.05,
        volume_ratio_max: float = 0.8,
```

并在 `__init__` 体中保存：

```python
        self.proximity_min = proximity_min
        self.volume_ratio_max = volume_ratio_max
```

完整 `__init__` 签名变为：

```python
    def __init__(
        self,
        ma_windows: List[int] = None,
        slope_window: int = 30,
        min_slope_pct: float = 0.0003,
        cross_window: int = 60,
        min_cross_count: int = 2,
        hist_days: int = 400,
        proximity_min: float = -0.05,
        volume_ratio_max: float = 0.8,
    ) -> None:
        self.ma_windows = ma_windows or [120, 250]
        self.slope_window = slope_window
        self.min_slope_pct = min_slope_pct
        self.cross_window = cross_window
        self.min_cross_count = min_cross_count
        self.hist_days = hist_days
        self.proximity_min = proximity_min
        self.volume_ratio_max = volume_ratio_max
```

- [ ] **Step 2: 在 Step 4 之后插入深度限制检查**

找到 `_analyze_stock` 中的这段代码（Step 4 紧接着 proximity 计算）：

```python
            proximity = current / current_ma - 1  # 负值
            triggered.append((window, slope_pct, proximity, sign_changes))
```

改为：

```python
            proximity = current / current_ma - 1  # 负值

            # Step 4a: 回踩深度不能超过 proximity_min
            if proximity < self.proximity_min:
                continue

            triggered.append((window, slope_pct, proximity, sign_changes))
```

- [ ] **Step 3: 运行测试，确认 proximity_min 相关测试通过**

```bash
python -m pytest tests/analysis/test_ma_trend_pullback_filter.py::test_proximity_min_filters_deep_pullback tests/analysis/test_ma_trend_pullback_filter.py::test_proximity_min_passes_shallow_pullback -v
```

期望：2 passed

- [ ] **Step 4: Commit**

```bash
git add src/analysis/ma_trend_pullback_filter.py tests/analysis/test_ma_trend_pullback_filter.py
git commit -m "feat: add proximity_min filter to MATrendPullbackFilter"
```

---

## Task 3: 为 volume_ratio_max 写失败测试

**Files:**
- Modify: `tests/analysis/test_ma_trend_pullback_filter.py`

- [ ] **Step 1: 追加两个成交量相关测试**

在测试文件末尾追加：

```python
def test_volume_ratio_max_filters_high_volume():
    """放量回踩（vol_ratio >= volume_ratio_max）应被过滤。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98   # 回踩 2%

    # 放量：近 5 日 = 前 20 日均量（vol_ratio = 1.0 >= 0.8）
    volumes = np.ones(400) * 100.0
    volumes[-5:] = 100.0               # 不缩量

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    assert f._analyze_stock(hist) is None


def test_volume_ratio_max_passes_low_volume():
    """缩量回踩（vol_ratio < volume_ratio_max）应通过成交量过滤。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98

    volumes = np.ones(400) * 100.0
    volumes[-5:] = 50.0                # vol_ratio = 0.5 < 0.8

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        'volume': volumes,
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    result = f._analyze_stock(hist)
    assert result is not None


def test_volume_filter_skipped_when_no_volume_column():
    """hist 无 volume 列时，成交量过滤静默跳过，不报错。"""
    dates = pd.date_range('2023-01-01', periods=400, freq='B')
    base = np.linspace(8.0, 12.0, 400)
    cross_zone = np.sin(np.linspace(0, 3 * np.pi, 61)) * 0.5
    prices = base.copy()
    prices[-61:] = base[-61:] + cross_zone
    ma120_approx = np.mean(prices[-120:])
    prices[-1] = ma120_approx * 0.98

    hist = pd.DataFrame({
        'date': dates,
        'open': prices, 'close': prices,
        'high': prices * 1.005, 'low': prices * 0.995,
        # 故意不包含 volume 列
    })
    f = MATrendPullbackFilter(
        volume_ratio_max=0.8,
        proximity_min=-0.10,
        min_slope_pct=0.0,
        min_cross_count=0,
    )
    result = f._analyze_stock(hist)
    assert result is not None  # 无 volume 列时不过滤
```

- [ ] **Step 2: 运行，确认失败**

```bash
python -m pytest tests/analysis/test_ma_trend_pullback_filter.py::test_volume_ratio_max_filters_high_volume tests/analysis/test_ma_trend_pullback_filter.py::test_volume_ratio_max_passes_low_volume tests/analysis/test_ma_trend_pullback_filter.py::test_volume_filter_skipped_when_no_volume_column -v
```

期望：`test_volume_ratio_max_filters_high_volume` FAILED（放量未被拦截），其余 2 个因逻辑未实现而意外通过（回归守卫测试）

---

## Task 4: 实现 volume_ratio_max 过滤

**Files:**
- Modify: `src/analysis/ma_trend_pullback_filter.py`

- [ ] **Step 1: 在 `_analyze_stock` 开头提取 volume 数组（可选列）**

在 `closes = hist['close'].astype(float).values` 后加一行：

```python
        volumes = hist['volume'].astype(float).values if 'volume' in hist.columns else None
```

- [ ] **Step 2: 在 Step 4a 之后插入成交量过滤**

在 `# Step 4a` 块之后，`triggered.append(...)` 之前，插入：

```python
            # Step 4b: 缩量回踩确认
            if volumes is not None and len(volumes) >= 25:
                vol_pullback = float(np.mean(volumes[-5:]))
                vol_base = float(np.mean(volumes[-25:-5]))
                if vol_base > 0 and vol_pullback / vol_base >= self.volume_ratio_max:
                    continue
```

完整的 for 循环末尾（Step 4 之后）此时结构为：

```python
            # Step 4: 当前价 < 均线
            if current >= current_ma:
                continue

            proximity = current / current_ma - 1  # 负值

            # Step 4a: 回踩深度不能超过 proximity_min
            if proximity < self.proximity_min:
                continue

            # Step 4b: 缩量回踩确认
            if volumes is not None and len(volumes) >= 25:
                vol_pullback = float(np.mean(volumes[-5:]))
                vol_base = float(np.mean(volumes[-25:-5]))
                if vol_base > 0 and vol_pullback / vol_base >= self.volume_ratio_max:
                    continue

            triggered.append((window, slope_pct, proximity, sign_changes))
```

- [ ] **Step 3: 运行全部测试，全部通过**

```bash
python -m pytest tests/analysis/test_ma_trend_pullback_filter.py -v
```

期望：所有测试 PASSED

- [ ] **Step 4: Commit**

```bash
git add src/analysis/ma_trend_pullback_filter.py tests/analysis/test_ma_trend_pullback_filter.py
git commit -m "feat: add volume_ratio_max filter to MATrendPullbackFilter"
```

---

## Task 5: 创建回测脚本 run_backtest.py

**Files:**
- Create: `run_backtest.py`

- [ ] **Step 1: 新建脚本**

```python
# run_backtest.py
"""
快速回测脚本：对每只股票历史数据滚动回放，统计 MATrendPullbackFilter 信号的
固定持有 N 日收益表现。

用法：
    python run_backtest.py [--hold_days N] [--volume_filter on|off] [--proximity_min F]

示例：
    python run_backtest.py --hold_days 10
    python run_backtest.py --hold_days 20 --volume_filter off
"""

import argparse
import asyncio
import logging
import sys
import time
from typing import List

import aiohttp
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

from src.data._cache import StockCache
from src.data.universe_fetcher import get_csi300
from src.data import AsyncStockDataFetcher
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='MA Trend Pullback 快速回测')
    p.add_argument('--hold_days', type=int, default=10, help='固定持有天数（默认 10）')
    p.add_argument('--volume_filter', choices=['on', 'off'], default='on',
                   help='是否启用成交量缩量过滤（默认 on）')
    p.add_argument('--proximity_min', type=float, default=-0.05,
                   help='回踩深度下限，如 -0.05 表示最多跌 5%%（默认 -0.05）')
    return p.parse_args()


def _backtest_one(hist: pd.DataFrame, f: MATrendPullbackFilter, hold_days: int) -> List[float]:
    """对单只股票历史数据滚动回放，返回每个信号的持有收益率列表。"""
    closes = hist['close'].astype(float).values
    n = len(closes)
    min_start = max(f.ma_windows) + f.slope_window
    returns = []
    for t in range(min_start, n - hold_days):
        sub = hist.iloc[:t].reset_index(drop=True)
        if f._analyze_stock(sub) is not None:
            buy = closes[t]
            sell = closes[t + hold_days]
            if buy > 0:
                returns.append((sell - buy) / buy)
    return returns


def _print_summary(all_returns: List[float], hold_days: int, volume_filter: str, proximity_min: float) -> None:
    if not all_returns:
        print('\n无有效信号，无法统计。')
        return
    arr = np.array(all_returns)
    win_rate = (arr > 0).mean() * 100
    avg_ret = arr.mean() * 100
    med_ret = np.median(arr) * 100
    max_loss = arr.min() * 100
    print(f'\n===== 回测结果 (持有 {hold_days} 日, 成交量过滤={volume_filter}, 深度限制={proximity_min*100:.1f}%) =====')
    print(f'信号总数:       {len(arr)}')
    print(f'胜率:           {win_rate:.1f}%')
    print(f'平均收益:      {avg_ret:+.2f}%')
    print(f'中位收益:      {med_ret:+.2f}%')
    print(f'最大单笔亏损:  {max_loss:+.2f}%')


async def _fetch_all_hist(stock_codes: List[str], hist_days: int) -> dict:
    fetcher = AsyncStockDataFetcher()
    fetcher.semaphore = asyncio.Semaphore(20)
    result = {}
    async with aiohttp.ClientSession() as session:
        tasks = [fetcher.get_stock_historical_data(session, code, hist_days=hist_days) for code in stock_codes]
        hists = await asyncio.gather(*tasks)
    for code, hist in zip(stock_codes, hists):
        if hist is not None and not hist.empty:
            result[code] = hist
    return result


def main() -> None:
    args = _parse_args()
    cache = StockCache()
    stocks = get_csi300(cache)
    if not stocks:
        print('沪深300成分股获取失败')
        sys.exit(1)

    volume_ratio = 0.8 if args.volume_filter == 'on' else 999.0  # 999 = 实质关闭
    f = MATrendPullbackFilter(
        proximity_min=args.proximity_min,
        volume_ratio_max=volume_ratio,
    )

    stock_codes = [s['code'] for s in stocks]
    hist_days = max(f.ma_windows) + f.slope_window + args.hold_days + 20  # 留足余量
    hist_days = max(hist_days, 400)

    logger.info('获取 %d 只股票历史数据（%d 日）...', len(stock_codes), hist_days)
    t0 = time.time()
    hist_map = asyncio.run(_fetch_all_hist(stock_codes, hist_days))
    logger.info('数据获取完成，用时 %.1fs，有效 %d 只', time.time() - t0, len(hist_map))

    logger.info('开始回放信号...')
    all_returns: List[float] = []
    for code, hist in hist_map.items():
        rets = _backtest_one(hist, f, args.hold_days)
        all_returns.extend(rets)

    _print_summary(all_returns, args.hold_days, args.volume_filter, args.proximity_min)


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: 快速冒烟测试（用 2 只股票验证不崩溃）**

```bash
python -c "
import pandas as pd
import numpy as np
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter
from run_backtest import _backtest_one

n = 420
dates = pd.date_range('2022-01-01', periods=n, freq='B')
base = np.linspace(8.0, 13.0, n)
prices = base.copy()
# sin 波制造穿越
cross = np.sin(np.linspace(0, 6*np.pi, 61)) * 0.3
prices[-61:] += cross
prices[-1] = np.mean(prices[-120:]) * 0.97
volumes = np.ones(n) * 100.0
volumes[-5:] = 40.0
hist = pd.DataFrame({'date': dates, 'open': prices, 'close': prices,
                     'high': prices*1.005, 'low': prices*0.995, 'volume': volumes})
f = MATrendPullbackFilter()
rets = _backtest_one(hist, f, hold_days=10)
print(f'信号数: {len(rets)}, 收益: {rets[:3]}')
"
```

期望：打印出信号数和收益列表，不报错

- [ ] **Step 3: Commit**

```bash
git add run_backtest.py
git commit -m "feat: add run_backtest.py for MA trend pullback quick backtest"
```

---

## Task 6: 最终验证

- [ ] **Step 1: 运行完整测试套件**

```bash
python -m pytest tests/ -v
```

期望：所有测试 PASSED，无 ERROR

- [ ] **Step 2: 验证 Filter 默认参数与之前行为一致（回归检查）**

```bash
python -c "
from src.analysis.ma_trend_pullback_filter import MATrendPullbackFilter
f = MATrendPullbackFilter()
print('proximity_min:', f.proximity_min)    # 期望 -0.05
print('volume_ratio_max:', f.volume_ratio_max)  # 期望 0.8
print('ma_windows:', f.ma_windows)         # 期望 [120, 250]
"
```

- [ ] **Step 3: Final commit**

```bash
git add -A
git status  # 确认无意外文件
git commit -m "feat: MA trend pullback optimization — proximity limit + volume filter + backtest" \
  --allow-empty-message || true
# 如果无未提交变更，此步可跳过
```
