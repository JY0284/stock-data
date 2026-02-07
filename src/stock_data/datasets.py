from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatasetInfo:
    name: str
    category: str  # e.g. basic / market
    partitioning: str  # e.g. trade_date / snapshot
    source: str  # tushare endpoint / concept
    desc_en: str
    desc_zh: str


DATASETS: list[DatasetInfo] = [
    DatasetInfo(
        name="stock_basic",
        category="basic",
        partitioning="snapshot",
        source="tushare: stock_basic",
        desc_en="Listed security master data (stock list/universe; one row per ts_code with name/industry/list status/dates, etc.).",
        desc_zh="股票列表/证券基础信息（每个 ts_code 一行；名称/行业/上市状态/上市退市日期等）。",
    ),
    DatasetInfo(
        name="trade_cal",
        category="basic",
        partitioning="snapshot",
        source="tushare: trade_cal",
        desc_en="Exchange trading calendar (open/close days), used to generate correct trading-date partitions.",
        desc_zh="交易所交易日历（开市/休市），用于生成正确的交易日分区。",
    ),
    DatasetInfo(
        name="stock_company",
        category="basic",
        partitioning="snapshot",
        source="tushare: stock_company",
        desc_en="Listed company profile per ts_code (management, location, website, business scope, etc.).",
        desc_zh="上市公司基本信息（管理层、地区、官网、主营业务/经营范围等）。",
    ),
    DatasetInfo(
        name="new_share",
        category="basic",
        partitioning="window (year)",
        source="tushare: new_share",
        desc_en="IPO/new listing information (stored as year windows).",
        desc_zh="IPO 新股列表/新股发行信息（按年窗口存储）。",
    ),
    DatasetInfo(
        name="namechange",
        category="basic",
        partitioning="window (year)",
        source="tushare: namechange",
        desc_en="Security historical names / name change events (stored as year windows).",
        desc_zh="股票曾用名/更名事件（按年窗口存储）。",
    ),
    DatasetInfo(
        name="daily",
        category="market",
        partitioning="trade_date",
        source="tushare: daily",
        desc_en="Daily OHLCV bars (per ts_code) for a given trading day.",
        desc_zh="日线行情（指定交易日，全市场股票日线 OHLCV）。",
    ),
    DatasetInfo(
        name="adj_factor",
        category="market",
        partitioning="trade_date",
        source="tushare: adj_factor",
        desc_en="Adjustment factors per ts_code and trade_date (used for qfq/hfq price adjustment).",
        desc_zh="复权因子（ts_code + trade_date，用于前/后复权计算）。",
    ),
    DatasetInfo(
        name="daily_basic",
        category="market",
        partitioning="trade_date",
        source="tushare: daily_basic",
        desc_en="Daily market indicators per ts_code and trade_date (turnover, valuation, shares, market cap, etc.).",
        desc_zh="每日行情指标（换手率、估值指标、股本、市值等）。",
    ),
    DatasetInfo(
        name="weekly",
        category="market",
        partitioning="trade_date (week-end)",
        source="tushare: weekly",
        desc_en="Weekly bars keyed by the week-end trading date.",
        desc_zh="周线行情（以当周最后一个交易日作为 trade_date）。",
    ),
    DatasetInfo(
        name="monthly",
        category="market",
        partitioning="trade_date (month-end)",
        source="tushare: monthly",
        desc_en="Monthly bars keyed by the month-end trading date.",
        desc_zh="月线行情（以当月最后一个交易日作为 trade_date）。",
    ),
    DatasetInfo(
        name="suspend_d",
        category="market",
        partitioning="trade_date",
        source="tushare: suspend_d",
        desc_en="Suspension/resumption events on a trade date (we fetch both suspend types and combine).",
        desc_zh="停复牌信息（按交易日；会合并不同 suspend_type 的结果）。",
    ),
    DatasetInfo(
        name="stk_limit",
        category="market",
        partitioning="trade_date",
        source="tushare: stk_limit",
        desc_en="Daily limit-up/limit-down prices (price bands) per ts_code and trade_date.",
        desc_zh="涨跌停价格（按交易日；包含涨停价/跌停价）。",
    ),
    DatasetInfo(
        name="index_basic",
        category="basic",
        partitioning="snapshot",
        source="tushare: index_basic",
        desc_en="Index basic info (指数基础信息) for all major indices.",
        desc_zh="指数基础信息（全市场主要指数）。",
    ),
    DatasetInfo(
        name="index_daily",
        category="market",
        partitioning="ts_code (snapshot)",
        source="tushare: index_daily",
        desc_en="Index daily bars per ts_code (covers major exchange indices). Partitioned by ts_code, each file contains full history.",
        desc_zh="指数日线行情（按指数代码；每个文件包含该指数完整历史数据）。",
    ),
    DatasetInfo(
        name="etf_daily",
        category="market",
        partitioning="trade_date",
        source="tushare: fund_daily (ETF/场内基金)",
        desc_en="ETF/Exchange-traded fund daily bars for a given trade_date (Tushare fund_daily).",
        desc_zh="ETF/场内基金日线行情（指定交易日，使用 Tushare fund_daily 接口）。",
    ),
    # ETF专题 datasets
    DatasetInfo(
        name="fund_basic",
        category="basic",
        partitioning="snapshot",
        source="tushare: fund_basic (market=E)",
        desc_en="ETF basic info (基金基础信息) for all exchange-traded funds.",
        desc_zh="ETF基础信息（全市场ETF/场内基金）。",
    ),
    DatasetInfo(
        name="fund_nav",
        category="etf",
        partitioning="ts_code (snapshot)",
        source="tushare: fund_nav",
        desc_en="ETF net asset value (单位净值) per ts_code. Partitioned by ts_code, each file contains full history.",
        desc_zh="ETF单位净值（按基金代码；每个文件包含该基金完整净值历史）。",
    ),
    DatasetInfo(
        name="fund_share",
        category="etf",
        partitioning="ts_code (snapshot)",
        source="tushare: fund_share",
        desc_en="ETF shares outstanding (份额变动) per ts_code. Partitioned by ts_code, each file contains full history.",
        desc_zh="ETF份额变动（按基金代码；每个文件包含该基金完整份额历史）。",
    ),
    DatasetInfo(
        name="fund_div",
        category="etf",
        partitioning="ts_code (snapshot)",
        source="tushare: fund_div",
        desc_en="ETF dividend distribution (分红送配) per ts_code. Partitioned by ts_code, each file contains full history.",
        desc_zh="ETF分红送配（按基金代码；每个文件包含该基金完整分红历史）。",
    ),
    DatasetInfo(
        name="income",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: income_vip",
        desc_en="Income statement (利润表) per ts_code and report period (quarterly/annual financial data, requires 5000 points).",
        desc_zh="利润表（按报告期；季度/年度财务数据，需要 5000 积分）。",
    ),
    DatasetInfo(
        name="balancesheet",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: balancesheet_vip",
        desc_en="Balance sheet (资产负债表) per ts_code and report period (quarterly/annual financial data, requires 5000 points).",
        desc_zh="资产负债表（按报告期；季度/年度财务数据，需要 5000 积分）。",
    ),
    DatasetInfo(
        name="cashflow",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: cashflow_vip",
        desc_en="Cash flow statement (现金流量表) per ts_code and report period (quarterly/annual financial data, requires 5000 points).",
        desc_zh="现金流量表（按报告期；季度/年度财务数据，需要 5000 积分）。",
    ),
    DatasetInfo(
        name="forecast",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: forecast_vip",
        desc_en="Earnings forecast (业绩预告) per ts_code and report period (requires 5000 points).",
        desc_zh="业绩预告（按报告期；需要 5000 积分）。",
    ),
    DatasetInfo(
        name="express",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: express_vip",
        desc_en="Earnings express (业绩快报) per ts_code and report period (requires 5000 points).",
        desc_zh="业绩快报（按报告期；需要 5000 积分）。",
    ),
    DatasetInfo(
        name="dividend",
        category="finance",
        partitioning="ts_code (snapshot)",
        source="tushare: dividend",
        desc_en="Dividend distribution (分红送股) per ts_code. Partitioned by ts_code, each file contains full history.",
        desc_zh="分红送股数据（按股票代码；每个文件包含该股票完整分红历史）。",
    ),
    DatasetInfo(
        name="fina_indicator",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: fina_indicator_vip",
        desc_en="Financial indicators (财务指标) per ts_code and report period (requires 5000 points).",
        desc_zh="财务指标数据（按报告期；需要 5000 积分）。",
    ),
    DatasetInfo(
        name="fina_audit",
        category="finance",
        partitioning="ts_code (snapshot)",
        source="tushare: fina_audit",
        desc_en="Financial audit opinion (财务审计意见) per ts_code. Partitioned by ts_code, each file contains full history.",
        desc_zh="财务审计意见（按股票代码；每个文件包含该股票完整审计意见历史）。",
    ),
    DatasetInfo(
        name="fina_mainbz",
        category="finance",
        partitioning="end_date (quarterly)",
        source="tushare: fina_mainbz_vip",
        desc_en="Main business composition (主营业务构成) per ts_code and report period (requires 5000 points).",
        desc_zh="主营业务构成（按报告期；需要 5000 积分）。",
    ),
    DatasetInfo(
        name="disclosure_date",
        category="finance",
        partitioning="snapshot",
        source="tushare: disclosure_date",
        desc_en="Financial report disclosure date schedule (财报披露日期表) for all listed companies.",
        desc_zh="财报披露日期表（全市场上市公司财报披露日期）。",
    ),

    # Macro (国内宏观) datasets
    DatasetInfo(
        name="lpr",
        category="macro",
        partitioning="snapshot",
        source="tushare: shibor_lpr",
        desc_en="Loan Prime Rate (LPR) time series (stored as a single snapshot parquet).",
        desc_zh="贷款市场报价利率 LPR 时间序列（单文件快照存储）。",
    ),
    DatasetInfo(
        name="cpi",
        category="macro",
        partitioning="snapshot",
        source="tushare: cn_cpi",
        desc_en="China CPI time series (stored as a single snapshot parquet).",
        desc_zh="中国 CPI 时间序列（单文件快照存储）。",
    ),
    DatasetInfo(
        name="cn_sf",
        category="macro",
        partitioning="snapshot",
        source="tushare: sf_month",
        desc_en="China social financing (社融) time series (stored as a single snapshot parquet).",
        desc_zh="中国社融时间序列（单文件快照存储）。",
    ),
    DatasetInfo(
        name="cn_m",
        category="macro",
        partitioning="snapshot",
        source="tushare: cn_m",
        desc_en="China money supply (货币供应量) time series (stored as a single snapshot parquet).",
        desc_zh="中国货币供应量时间序列（单文件快照存储）。",
    ),

    # US Stock datasets (美股数据)
    DatasetInfo(
        name="us_basic",
        category="us",
        partitioning="snapshot",
        source="tushare: us_basic",
        desc_en="US stock list (basic info: ts_code, name, classify, list_date, delist_date).",
        desc_zh="美股列表/基础信息（代码、名称、分类、上市退市日期）。",
    ),
    DatasetInfo(
        name="us_tradecal",
        category="us",
        partitioning="snapshot",
        source="tushare: us_tradecal",
        desc_en="US trading calendar (cal_date, is_open, pretrade_date).",
        desc_zh="美股交易日历（日期、是否交易、上一交易日）。",
    ),
    DatasetInfo(
        name="us_daily",
        category="us",
        partitioning="trade_date",
        source="tushare: us_daily",
        desc_en="US stock daily bars (OHLCV, vwap, turnover, valuation) for a given trade_date.",
        desc_zh="美股日线行情（指定交易日，OHLCV、成交量、估值指标等）。",
    ),
]


ALL_DATASET_NAMES: list[str] = [d.name for d in DATASETS]


def dataset_info_map() -> dict[str, DatasetInfo]:
    return {d.name: d for d in DATASETS}


def parse_datasets(datasets: str) -> list[str]:
    if datasets.strip().lower() in {"all", "*"}:
        return list(ALL_DATASET_NAMES)
    return [d.strip() for d in datasets.split(",") if d.strip()]


def print_datasets(*, lang: str = "both") -> None:
    lang = (lang or "both").lower()
    if lang not in {"both", "en", "zh"}:
        raise ValueError("lang must be one of: both, en, zh")

    if lang == "both":
        header = f"{'dataset':<14} {'category':<8} {'partitioning':<18} {'description (EN)':<52} description (中文)"
    elif lang == "en":
        header = f"{'dataset':<14} {'category':<8} {'partitioning':<18} description"
    else:
        header = f"{'dataset':<14} {'category':<8} {'partitioning':<18} 说明"

    print(header)
    print("-" * len(header))

    for d in DATASETS:
        if lang == "both":
            print(
                f"{d.name:<14} {d.category:<8} {d.partitioning:<18} "
                f"{d.desc_en:<52} {d.desc_zh}"
            )
        elif lang == "en":
            print(f"{d.name:<14} {d.category:<8} {d.partitioning:<18} {d.desc_en}")
        else:
            print(f"{d.name:<14} {d.category:<8} {d.partitioning:<18} {d.desc_zh}")
