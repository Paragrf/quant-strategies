# MA Trend Pullback 策略优化设计

**日期:** 2026-04-17  
**范围:** `src/analysis/ma_trend_pullback_filter.py` 增强 + 新增 `run_backtest.py`

## 背景

现有 `MATrendPullbackFilter` 是纯信号扫描器，无回测能力，无成交量确认，也不限制回踩深度。目标是通过三项优化提高信号质量并验证效果。

## 优化项

### 1. 回踩深度限制（proximity_min）

**位置:** `_analyze_stock`，Step 4（`price < MA`）之后，打分之前  
**参数:** `proximity_min: float = -0.05`（默认 -5%）  
**逻辑:** `if proximity < proximity_min: continue`  
**目的:** 剔除跌破均线幅度过大的标的，避免接趋势破坏后的飞刀

### 2. 缩量回踩确认（volume_ratio_max）

**位置:** `_analyze_stock`，深度限制检查之后  
**参数:** `volume_ratio_max: float = 0.8`（默认 0.8）  
**数据要求:** `hist` 需包含 `volume` 列  
**逻辑:**
```
# 相对于当前可见数据末尾（sub_hist 或完整 hist 的最后一日）
vol_pullback = mean(volume[-5:])          # 近 5 日均量（回踩段）
vol_base     = mean(volume[-25:-5])       # 前 20 日均量（上涨参照段）
if vol_base > 0 and vol_pullback / vol_base >= volume_ratio_max: continue
```
**目的:** 缩量回踩 = 获利盘不恐慌，放量回踩 = 可能是主力出货

### 3. 快速回测脚本（run_backtest.py）

**文件:** `run_backtest.py`（项目根目录，与其他 `run_*.py` 平级）  
**CLI 参数:**

| 参数 | 默认 | 说明 |
|------|------|------|
| `--hold_days` | `10` | 持有天数 |
| `--volume_filter` | `on` | 是否启用成交量过滤 |
| `--proximity_min` | `-0.05` | 回踩深度下限 |

**回测逻辑（向量化滚动窗口）:**
```
min_start = max(ma_windows) + slope_window  # 需足够数据计算 MA 和斜率
对每只股票 hist（400 日数据）:
  for t in range(min_start, len(hist)-hold_days):
      sub_hist = hist[:t]
      if _analyze_stock(sub_hist) is not None:
          ret = (closes[t+hold_days] - closes[t]) / closes[t]
          records.append(ret)
```

**输出（终端打印）:**
```
===== 回测结果 (持有 10 日) =====
信号总数:     142
胜率:         58.5%
平均收益:    +2.3%
中位收益:    +1.8%
最大单笔亏损: -9.2%
```

**对比模式:** `--volume_filter off` 可跑无过滤版，对比两者指标差异

## 数据依赖

- `get_stock_historical_data` 需返回包含 `volume` 列的 DataFrame（检查现有 fetcher 是否已包含）
- 若 `volume` 列不存在，`volume_filter` 自动降级为跳过（日志 warning，不报错）

## 不在本次范围内

- 参数网格搜索
- 权益曲线图
- 多仓持有模拟
- 止盈止损规则
