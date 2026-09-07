# 回测动态退出 + 参数调优 设计

**日期:** 2026-04-17  
**范围:** `src/analysis/ma_trend_pullback_filter.py`、`run_backtest.py`、`run_backtest_compare.py`

## 背景

回测数据显示固定持有 10 日胜率仅 38.7%，且"跌破均线买入"存在摸底过早问题。本次优化：
1. 将 `proximity_min` 默认值收紧至 -3%（回测数据支持，深度 -3% 组合均收益 +0.13%）
2. 回测退出逻辑从"固定 N 日"改为"动态三条件退出"

## 改动一：proximity_min 默认值

**文件:** `src/analysis/ma_trend_pullback_filter.py`  
**改动:** `proximity_min: float = -0.05` → `proximity_min: float = -0.03`  
**同步:** `_analyze_stock` 返回 dict 新增 `ma_window: int`（值为 `best_window`），供回测退出逻辑使用

## 改动二：动态退出逻辑（run_backtest.py）

### 新增函数 `_find_exit`

```
_find_exit(closes, t, window, max_hold_days, stop_loss) -> (sell_price, reason)
```

**逻辑（逐日检查 k = 1 → max_hold_days）：**

| 优先级 | 条件 | 动作 | reason |
|--------|------|------|--------|
| 1 | `closes[t+k] >= mean(closes[t+k-window : t+k])` | 收复均线止盈 | `'recovery'` |
| 2 | `(closes[t+k] - closes[t]) / closes[t] < stop_loss` | 止损 | `'stop_loss'` |
| 3 | `k == max_hold_days` | 到期强平 | `'max_hold'` |

**边界处理：** 若 `t + k >= len(closes)`，返回最后一日收盘价，reason = `'end_of_data'`

### `_backtest_one` 改造

```python
# 旧
sell = closes[t + hold_days]
returns.append((sell - buy) / buy)

# 新
sell, reason = _find_exit(closes, t, result['ma_window'], max_hold_days, stop_loss)
returns.append({'ret': (sell - buy) / buy, 'reason': reason})
```

### 新增 CLI 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--max_hold_days` | 30 | 最长持有天数（到期强平） |
| `--stop_loss` | -0.045 | 止损线（-0.03 × 1.5） |

`--hold_days` 参数从 `run_backtest.py` 移除（已被 `--max_hold_days` 替代）。`run_backtest_compare.py` 内部保留固定持有作为对比基准行，不对外暴露该参数。

### `_print_summary` 新增输出

```
收复均线离场: 45.2%
止损离场:     28.3%
到期强平:     26.5%
```

## 改动三：对比脚本（run_backtest_compare.py）

- `_backtest_one` 同步升级为动态退出版本
- 对比表新增三列：收复率 / 止损率 / 到期率

## 不在本次范围内

- 修改 `MATrendPullbackFilter.scan_stocks_sync`（扫描器没有退出逻辑）
- 多仓模拟、资金管理
- 固定持有模式的 CLI 参数（移除 `--hold_days`）
