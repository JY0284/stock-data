# Stock Data Lake（Tushare）

本项目用于将 Tushare A 股相关数据落地到本地数据湖，存储为 **DuckDB + 分区 Parquet**（便于增量更新、可恢复、可重复运行）。

设计目标（偏工程可用性）：
- **限流**：按每分钟请求数（rpm）控制远端调用。
- **并发**：按交易日分区并行下载。
- **可恢复**：DuckDB 记录 `ingestion_state`，支持中断后继续。
- **幂等**：同一分区 Parquet 以原子替换方式覆盖写入。

## 安装

1. **安装依赖**（推荐使用 `uv`，或使用 pip）：

```bash
# 使用 uv（推荐）
uv sync
source .venv/bin/activate

# 或使用 pip
pip install -e .
```

2. **设置 Tushare Token**：

```bash
export TUSHARE_TOKEN="your_token_here"
```

## 使用

命令行入口为 `stock-data`。

### 1）历史回填（Backfill）

下载一个明确的历史区间（例如 2010～2023），并行执行，耗时较长：

```bash
stock-data backfill --start-date 20100101 --end-date 20231231
```

### 2）日常更新（Update）

建议每天跑一次（例如定时任务/cron），用于从“上次成功的分区”往后补到今天：

```bash
stock-data update
```

说明：
- 有些数据集是 **按 `ts_code` 分区**且会持续增长的（例如 `index_daily`、`fund_nav`、`fund_share`）。在 `update` 模式下，如果本地已经存在对应的 `ts_code=...parquet`，会优先用 Tushare 的 `start_date/end_date` 只拉“缺失的尾部区间”，再与本地历史 merge+去重写回。
- 默认情况下，`update` 会以 `--end-date` 为目标，通过比较每个 `ts_code` 的本地最大日期来判断是否“落后”，并只拉取缺失的尾部数据，从而保证本地尽量追平上游。
- `STOCK_DATA_TS_CODE_REFRESH_DAYS` 是一个**可选的强制重刷**开关：如果设置（例如 `7`），即使已经追平也会周期性重新拉取（用于应对上游修订等情况）；设置为 `0` 表示关闭强制重刷。

默认 `update` 的 `end_date` 为 **今天**（本机日期）。也可以指定截止日期：

```bash
stock-data update --end-date 20260123
```

### 3）数据校验（Validate）

进行本地一致性/可读性检查；可选远端 spot-check（取决于命令参数）。

```bash
stock-data validate
```

如果你是在 macOS 上把 `store/` 打包（zip）再解压到 Linux，压缩包里可能会带上 AppleDouble 文件（形如 `._*.parquet`）。
DuckDB 的 `read_parquet()` 在做 glob 时可能会把这些文件也匹配进去，导致报错类似：
`Invalid Input Error: No magic bytes found ... '.../._something.parquet'`。

在目标机器上清理即可：

- Linux：
  - `find store -name '._*' -type f -delete`
  - `find store -name '.DS_Store' -type f -delete`
- macOS（打包前）：
  - `dot_clean -m store`

更稳妥的传输方式：

- `rsync -a --exclude='._*' --exclude='.DS_Store' store/ user@host:/path/to/store/`
- `COPYFILE_DISABLE=1 tar -czf store.tgz store`（在 Linux 解压）

你也可以直接用 CLI 清理已有的 store 目录：

```bash
# 预览（不删除）
stock-data clean-store --store store --dry-run

# 实际删除
stock-data clean-store --store store
```

### 4）SQL 查询（DuckDB）

可以直接对 DuckDB 执行 SQL。项目会创建一些视图（例如 `v_daily`、`v_adj_factor`、以及派生的 `v_daily_adj` 等）。

```bash
# 最新交易日
stock-data query --sql "SELECT MAX(trade_date) FROM v_daily"

# 查看某只股票的复权后收盘价（示例）
stock-data query --sql "SELECT trade_date, close, qfq_close FROM v_daily_adj WHERE ts_code='000001.SZ' ORDER BY trade_date DESC LIMIT 5"
```

### 5）统计（覆盖范围/体量）

```bash
# 全部数据集
stock-data stat

# 指定数据集
stock-data stat --datasets daily,adj_factor,daily_basic
```

### 6）列出数据集（说明）

```bash
# 默认双语
stock-data datasets

# 仅英文
stock-data datasets --lang en

# 仅中文
stock-data datasets --lang zh
```

### 7）从远端服务同步本地 store（Sync）

如果你有一台机器（例如服务器）上的 `store/` 是完整且最新的，可以在那台机器上启动 HTTP 服务，然后在本机把 `store/` 同步过来。

在 **远端** 机器（服务端）启动：

```bash
stock-data serve --store store --host 0.0.0.0 --port 8000
```

在 **本地** 机器（客户端）执行同步：

```bash
stock-data sync --store store --remote http://1.2.3.4:8000
```

如果远端是默认端口（http=80 / https=443），可以省略端口：

```bash
stock-data sync --store store --remote http://stock-data.example.com
```

说明：
- 同步范围只包含 `store/duckdb/` 与 `store/parquet/` 两个子目录。
- 默认按文件大小判断是否需要下载（更快，也能避免不同机器 mtime 不一致导致的全量重复下载）。
- `--hash` 会对每个文件做 sha256 校验（更慢但更严格/更安全）。
- `--dry-run` 仅预览变化。
- `--delete` 会删除“本地存在但远端不存在”的文件（危险操作，慎用）。

## Python API（推荐）

`StockStore` 提供了更易用的 Python 访问层：屏蔽 DuckDB 连接、Parquet 路径、以及常见查询的细节，并内置缓存与分区裁剪（适合频繁的小查询/后续接入 agent tools）。

```python
from stock_data.store import open_store

# 打开本地 store（默认开启缓存）
store = open_store("store")

# 解析 symbol -> ts_code
resolved = store.resolve("300888")  # -> 300888.SZ

# 读取日线
df = store.daily("300888.SZ", start_date="20240101", end_date="20240131")

# 复权价格（qfq=前复权）
df_adj = store.daily_adj("300888.SZ", start_date="20240101", how="qfq")

# 交易日历
days = store.trading_days("20240101", "20240131")
is_open = store.is_trading_day("20240115")

# 股票列表 / universe
universe = store.universe(list_status="L", market="创业板")

# 新股（IPO）信息
ipo = store.new_share(year=2024)

# 通用读取（兜底入口）
df = store.read("daily_basic", start_date="20240101", end_date="20240105")
```

特性：
- **分区裁剪**：按日期范围只读相关 Parquet 分区（避免全量 glob）。
- **缓存**：重复查询走内存缓存（默认 ~1GB）。
- **不依赖视图**：即使 DuckDB 视图不存在，也可以直接读 Parquet。

快速 Demo：

```bash
python demos/use_store_api.py 300888 store
```

## 数据读取

### 方式 A：DuckDB → pandas（推荐）

DuckDB 读取分区 Parquet 效率高，并能下推过滤条件（日期、列、代码等）。

```python
import duckdb

con = duckdb.connect("store/duckdb/market.duckdb")

df_daily = con.execute(
    """
    SELECT ts_code, trade_date, open, high, low, close, vol, amount
    FROM v_daily
    WHERE trade_date BETWEEN '20231225' AND '20231229'
    """
).fetchdf()
```

也可以直接对 Parquet 做 `read_parquet(...)`：

```python
import duckdb

con = duckdb.connect()

df = con.execute(
    """
    SELECT *
    FROM read_parquet('store/parquet/daily/**/[!.]*.parquet', union_by_name=true)
    WHERE trade_date = '20231226'
    LIMIT 10
    """
).fetchdf()
```

### 方式 B：Parquet → pandas（简单）

读取单个交易日分区：

```python
import pandas as pd

df = pd.read_parquet("store/parquet/daily/year=2023/month=12/trade_date=20231226.parquet")
```

## 存储布局

默认写入 `./store/`：

- `store/duckdb/market.duckdb`：目录与 `ingestion_state`（进度/状态）。
- `store/parquet/<dataset>/`：各数据集 Parquet。
  - `daily`、`adj_factor` 等：`year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`
  - `stock_basic`、`stock_company` 等：快照文件（例如 `latest.parquet`）

## Demo：单只股票拉全量数据

为了让你快速上手读取本地数据，这里提供了一个 demo：输入股票代码（例如 `300888`）或 `ts_code`（例如 `000001.SZ`），用 `StockStore` API 展示常见读取方式（包含解析代码、日线、复权、日历、universe 等）。

```bash
python demos/use_store_api.py 300888 store
```

如果你想看更“底层”的用法（直接 DuckDB/SQL 读多数据集并打印全量），仍可以使用旧的 demo：

```bash
python demos/print_stock_300888.py 300888 store
```

## FAQ / 概念

上游接口的分类概览可参考 Tushare：
- 基础数据：https://tushare.pro/document/2?doc_id=24
- 行情数据：https://tushare.pro/document/2?doc_id=15

### 1）粒度是什么？

- **行情类（market）数据集**：按 **交易日** 分区（每个交易日一个文件）。
  - `daily`、`adj_factor`、`daily_basic`、`stk_limit`、`suspend_d`
  - `weekly` / `monthly`：同样用 `trade_date` 分区，但只在周末/月底交易日生成分区
- **基础类（basic）数据集**：通常是快照或按年份窗口存储。
  - `stock_basic`、`trade_cal`、`stock_company`：快照
  - `new_share`、`namechange`：按年窗口

### 2）回填运行时能读/查吗？

可以。

- 读取 Parquet 通常是安全的：分区写入采用原子替换，读者一般只会看到旧文件或新文件。
- DuckDB 可能在写入时出现短暂等待；分析时建议优先查 Parquet 视图或直接 `read_parquet(...)`。

### 3）`backfill` / `update` / `validate` 分别做什么？

- **`backfill --start-date A --end-date B`**
  - 对所选数据集，在区间 **[A, B]** 内为每个开市交易日生成任务并下载。
  - 分区 Parquet 覆盖写入（幂等），适合做“指定区间的完整刷新”。

- **`update --end-date B`**
  - 增量模式：`B` 默认是 **今天**。
  - 对每个数据集，从该数据集的 **最后一个 completed 分区** 开始向后调度，补到 `B`。
  - 如果某数据集本地还没有任何 completed 分区，则只做最小化引导：抓取截至 `B` 的最新分区（不会自动回填全历史）。

- **`validate`**
  - 只做校验，不写入新数据。

### 4）同一个区间重复跑 `backfill` 与 `update` 有什么区别？

- 再跑一次 **`backfill A→B`**：会重新下载并覆盖该区间内的分区（更贵，但等价于强制刷新）。
- 跑 **`update --end-date B`**：若都已完成，基本无事可做；否则只补缺失/失败分区（便宜且适合日常）。

### 5）如何确认 Tushare 实际请求了哪些区间？

设置 `STOCK_DATA_LOG_TUSHARE_QUERY=1` 可以打印精简后的请求参数（用于验证增量拉取是否带了 `start_date/end_date` 或 `start_m/end_m`）：

```bash
STOCK_DATA_LOG_TUSHARE_QUERY=1 stock-data update --datasets index_daily --end-date 20260204
```

日志示例：

```text
tushare: query api=index_daily end_date=20260203 start_date=20260120 ts_code=000001.SH
```

## 数据集与字段（Schemas）

本节列的是“本仓库实际写入 Parquet 的字段”（不是把上游文档整段复制）。如果你本地 store 与示例不同，以你本地 Parquet schema 为准。

### 行情类（按交易日分区）

文件路径：`store/parquet/<dataset>/year=YYYY/month=MM/trade_date=YYYYMMDD.parquet`

#### `daily`（tushare: `daily`）

字段：
- `ts_code`, `trade_date`
- `open`, `high`, `low`, `close`
- `pre_close`, `change`, `pct_chg`
- `vol`, `amount`

#### `adj_factor`（tushare: `adj_factor`）

字段：
- `ts_code`, `trade_date`, `adj_factor`

#### `daily_basic`（tushare: `daily_basic`）

字段：
- `ts_code`, `trade_date`, `close`
- `turnover_rate`, `turnover_rate_f`, `volume_ratio`
- `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`
- `dv_ratio`, `dv_ttm`
- `total_share`, `float_share`, `free_share`
- `total_mv`, `circ_mv`

#### `weekly`（tushare: `weekly`，周末交易日为分区键）

字段：
- `ts_code`, `trade_date`
- `open`, `high`, `low`, `close`
- `pre_close`, `change`, `pct_chg`
- `vol`, `amount`

#### `monthly`（tushare: `monthly`，月底交易日为分区键）

字段：
- `ts_code`, `trade_date`
- `open`, `high`, `low`, `close`
- `pre_close`, `change`, `pct_chg`
- `vol`, `amount`

#### `stk_limit`（tushare: `stk_limit`）

字段：
- `trade_date`, `ts_code`, `up_limit`, `down_limit`

#### `suspend_d`（tushare: `suspend_d`）

字段：
- `ts_code`, `trade_date`, `suspend_timing`, `suspend_type`

### 按 ts_code 分区（单代码全量历史文件）

文件路径：`store/parquet/<dataset>/ts_code=<TS_CODE_下划线替换点号>.parquet`

这类数据集通常会通过 `start_date/end_date` 做“尾部增量拉取”，并与本地历史 merge+去重。

#### `index_daily`（tushare: `index_daily`）

常见字段：
- `ts_code`, `trade_date`
- `close`（以及上游可能返回的其他 OHLCV 字段）

#### `fund_nav`（tushare: `fund_nav`）

常见字段：
- `ts_code`, `nav_date`
- `adj_nav`（以及上游可能返回的其他净值字段）

#### `fund_share`（tushare: `fund_share`）

常见字段：
- `ts_code`, `trade_date`
- `fd_share`（以及上游可能返回的其他份额字段）

### 基础类（快照/按年窗口）

#### `stock_basic`（tushare: `stock_basic`）

文件：`store/parquet/stock_basic/latest.parquet`

字段：
- `ts_code`, `symbol`, `name`, `area`, `industry`
- `fullname`, `enname`, `cnspell`
- `market`, `exchange`, `curr_type`
- `list_status`, `list_date`, `delist_date`
- `is_hs`, `act_name`, `act_ent_type`

#### `trade_cal`（tushare: `trade_cal`）

文件：`store/parquet/trade_cal/SSE_latest.parquet`

字段：
- `exchange`, `cal_date`, `is_open`, `pretrade_date`

#### `stock_company`（tushare: `stock_company`）

文件：`store/parquet/stock_company/latest.parquet`

字段：
- `ts_code`, `com_name`, `com_id`, `exchange`
- `chairman`, `manager`, `secretary`
- `reg_capital`, `setup_date`
- `province`, `city`
- `introduction`, `website`, `email`, `office`
- `employees`, `main_business`, `business_scope`

#### `new_share`（tushare: `new_share`，按年窗口）

文件：`store/parquet/new_share/year=YYYY.parquet`

字段：
- `ts_code`, `sub_code`, `name`
- `ipo_date`, `issue_date`
- `amount`, `market_amount`
- `price`, `pe`
- `limit_amount`, `funds`, `ballot`

#### `namechange`（tushare: `namechange`，按年窗口）

文件：`store/parquet/namechange/year=YYYY.parquet`

字段：
- `ts_code`, `name`, `start_date`, `end_date`, `ann_date`, `change_reason`

### 财务类（按报告期分区）

文件路径：`store/parquet/<dataset>/year=YYYY/quarter=Q/end_date=YYYYMMDD.parquet`

财务数据集按 **报告期**（`end_date`）分区，而不是交易日。例如：
- 2024 年 Q1：`end_date=20240331`
- 2024 年 Q2：`end_date=20240630`
- 2024 年 Q3：`end_date=20240930`
- 2024 年年报：`end_date=20241231`

**注意**：这些数据集需要 **5000 积分**才能使用 VIP 接口获取全市场季度数据。

#### `income`（tushare: `income_vip`）

利润表。主要字段包括：
- `ts_code`, `end_date`, `ann_date`, `f_ann_date`
- `report_type`, `comp_type`, `end_type`
- `basic_eps`, `diluted_eps`：每股收益
- `total_revenue`, `revenue`：营业收入
- `total_cogs`, `oper_cost`：成本
- `operate_profit`, `total_profit`, `n_income`：利润指标
- 以及更多财务科目...

#### `balancesheet`（tushare: `balancesheet_vip`）

资产负债表。主要字段包括：
- `ts_code`, `end_date`, `ann_date`, `f_ann_date`
- `report_type`, `comp_type`, `end_type`
- `total_assets`, `total_cur_assets`, `total_nca`：资产指标
- `total_liab`, `total_cur_liab`, `total_ncl`：负债指标
- `total_hldr_eqy_exc_min_int`：股东权益
- 以及更多资产负债表科目...

#### `cashflow`（tushare: `cashflow_vip`）

现金流量表。主要字段包括：
- `ts_code`, `end_date`, `ann_date`, `f_ann_date`
- `report_type`, `comp_type`, `end_type`
- `n_cashflow_act`：经营活动现金流量净额
- `n_cashflow_inv_act`：投资活动现金流量净额
- `n_cashflow_fnc_act`：筹资活动现金流量净额
- `c_cash_equ_end_period`：期末现金及现金等价物余额
- 以及更多现金流量表科目...

#### `forecast`（tushare: `forecast_vip`）

业绩预告。主要字段：
- `ts_code`, `ann_date`, `end_date`
- `type`：预告类型（预增/预减/扭亏/首亏/续亏/续盈/略增/略减）
- `p_change_min`, `p_change_max`：净利润变动幅度范围（%）
- `net_profit_min`, `net_profit_max`：净利润范围（万元）
- `last_parent_net`：上年同期净利润
- `summary`, `change_reason`：预告摘要和变动原因

#### `express`（tushare: `express_vip`）

业绩快报。主要字段：
- `ts_code`, `ann_date`, `end_date`
- `revenue`, `operate_profit`, `total_profit`, `n_income`：关键财务数据
- `total_assets`, `total_hldr_eqy_exc_min_int`：资产负债表项目
- `diluted_eps`, `diluted_roe`, `bps`：每股指标
- `yoy_sales`, `yoy_op`, `yoy_tp`, `yoy_dedu_np`：同比增长率
- `perf_summary`, `is_audit`, `remark`：附加信息

#### `dividend`（tushare: `dividend`）

分红送股数据。主要字段：
- `ts_code`, `end_date`, `ann_date`, `div_proc`
- `stk_div`, `stk_bo_rate`, `stk_co_rate`：股票股利指标
- `cash_div`, `cash_div_tax`：现金股利指标
- `record_date`, `ex_date`, `pay_date`：重要日期
- `div_listdate`, `imp_ann_date`

#### `fina_indicator`（tushare: `fina_indicator_vip`）

财务指标数据。主要字段：
- `ts_code`, `end_date`, `ann_date`
- `eps`, `dt_eps`, `total_revenue_ps`, `revenue_ps`：每股指标
- `capital_rese_ps`, `surplus_rese_ps`, `undist_profit_ps`
- `extra_item`, `profit_dedt`, `gross_margin`：盈利能力
- `current_ratio`, `quick_ratio`, `cash_ratio`：流动性
- `ar_turn`, `ca_turn`, `fa_turn`, `assets_turn`：周转率
- `roe`, `roa`, `roe_waa`, `roe_dt`：回报率指标
- 以及更多财务比率和指标...

#### `fina_audit`（tushare: `fina_audit`）

财务审计意见。主要字段：
- `ts_code`, `ann_date`, `end_date`
- `audit_result`：审计结果
- `audit_fees`：审计费用
- `audit_agency`：会计事务所
- `audit_sign`：签字会计师

#### `fina_mainbz`（tushare: `fina_mainbz_vip`）

主营业务构成。主要字段：
- `ts_code`, `end_date`, `bz_item`, `bz_sales`, `bz_profit`, `bz_cost`
- `curr_type`, `update_flag`

#### `disclosure_date`（tushare: `disclosure_date`）

财报披露日期表。这是一个 **快照** 数据集（不按 `end_date` 分区）。

文件：`store/parquet/disclosure_date/latest.parquet`

主要字段：
- `ts_code`, `ann_date`, `end_date`
- `pre_date`, `actual_date`
- `modify_date`

### 使用财务数据

**Python API**：

```python
from stock_data.store import open_store

store = open_store("store")

# 获取利润表
df = store.income("300888.SZ", start_period="20230101", end_period="20231231")

# 获取资产负债表
df = store.balancesheet("300888.SZ", start_period="20230101", end_period="20231231")

# 获取现金流量表
df = store.cashflow("300888.SZ", start_period="20230101", end_period="20231231")

# 获取业绩预告
df = store.forecast("300888.SZ", start_period="20230101", end_period="20231231")

# 获取业绩快报
df = store.express("300888.SZ", start_period="20230101", end_period="20231231")
```

**SQL 查询**：

```bash
# 查看最新报告期
stock-data query --sql "SELECT MAX(end_date) FROM v_income"

# 获取某只股票的利润表
stock-data query --sql "SELECT ts_code, end_date, total_revenue, n_income FROM v_income WHERE ts_code='000001.SZ' ORDER BY end_date DESC LIMIT 5"
```

## 开发

- 源码：`src/stock_data/`
- 依赖与配置：`pyproject.toml`
