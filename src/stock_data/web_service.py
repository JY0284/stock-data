from __future__ import annotations

import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response

from stock_data.datasets import DATASETS, ALL_DATASET_NAMES, dataset_info_map
from stock_data.runner import RunConfig
from stock_data.stats import fetch_stats_json
from stock_data.store import StockStore, open_store


MacroDataset = Literal["lpr", "cpi", "cn_sf", "cn_m"]
UsDataset = Literal["us_basic", "us_tradecal", "us_daily"]


@dataclass(frozen=True)
class WebSettings:
    store_dir: str = os.environ.get("STOCK_DATA_STORE_DIR", "store")

    # Safety / reliability: avoid blowing up memory by default.
    default_limit: int = int(os.environ.get("STOCK_DATA_API_DEFAULT_LIMIT", "5000"))
    max_limit: int = int(os.environ.get("STOCK_DATA_API_MAX_LIMIT", "200000"))

    cache_enabled: bool = os.environ.get("STOCK_DATA_API_CACHE", "1").strip() not in {"0", "false", "False"}
    cache_budget_bytes: int = int(os.environ.get("STOCK_DATA_CACHE_BUDGET_BYTES", str(1_000_000_000)))
    cache_ttl_seconds: int = int(os.environ.get("STOCK_DATA_CACHE_TTL_SECONDS", str(10 * 60)))

    exchange_default: str = os.environ.get("STOCK_DATA_EXCHANGE_DEFAULT", "SSE")


def _parse_columns(columns: str | None) -> list[str] | None:
    if not columns:
        return None
    cols = [c.strip() for c in columns.split(",") if c.strip()]
    return cols or None


def _parse_where_json(where_json: str | None) -> dict[str, Any] | None:
    if where_json is None or not where_json.strip():
        return None
    try:
        obj = json.loads(where_json)
    except json.JSONDecodeError as e:  # pragma: no cover
        raise ValueError(f"Invalid where JSON: {e}") from e

    if not isinstance(obj, dict):
        raise ValueError("where must be a JSON object")

    for k in obj.keys():
        if not isinstance(k, str) or not k.strip():
            raise ValueError("where keys must be non-empty strings")

    # Values are passed into DuckDB parameter binding; keep them JSON-scalars or arrays.
    def _valid_value(v: Any) -> bool:
        if v is None:
            return True
        if isinstance(v, (str, int, float, bool)):
            return True
        if isinstance(v, list):
            return all(_valid_value(x) and not isinstance(x, list) and not isinstance(x, dict) for x in v)
        return False

    for v in obj.values():
        if not _valid_value(v):
            raise ValueError("where values must be JSON scalars or arrays of scalars")

    return obj


def _clamp_limit(limit: int | None, *, default_limit: int, max_limit: int) -> int:
    if limit is None:
        return int(default_limit)
    lim = int(limit)
    if lim <= 0:
        raise ValueError("limit must be > 0")
    return min(lim, int(max_limit))


def _df_to_json_records(df) -> list[dict[str, Any]]:
    """Convert a DataFrame to JSON-safe records.

    Starlette's JSONResponse is strict (disallows NaN/Infinity). Some datasets
    (e.g. fund_basic) may contain NaN; use DataFrame.to_json to normalize them
    to null before returning.
    """
    # Import lazily to keep CLI help lightweight.
    import json as _json

    # pandas DataFrame has to_json; this converts NaN/NaT to null in JSON.
    return _json.loads(df.to_json(orient="records", date_format="iso"))


def _html_index(settings: WebSettings, *, base_url: str) -> str:
        datasets = "\n".join(
                f"<li><code>{d.name}</code> — {d.desc_zh} <span class=\"muted\">/ {d.desc_en}</span></li>" for d in DATASETS
        )

        base_url = (base_url or "/").rstrip("/") + "/"

        return f"""<!doctype html>
<html lang=\"zh-CN\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>stock_data 数据服务</title>
    <style>
            :root {{ --bg: #ffffff; --fg: #111827; --muted: #6b7280; --card: #f6f8fa; --border: #e5e7eb; --link: #2563eb; }}
            body {{ background: var(--bg); color: var(--fg); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 2rem; line-height: 1.55; }}
            a {{ color: var(--link); text-decoration: none; }}
            a:hover {{ text-decoration: underline; }}
            code, pre {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }}
            pre {{ background: var(--card); border: 1px solid var(--border); padding: 1rem; overflow: auto; border-radius: 10px; }}
            .muted {{ color: var(--muted); }}
            .grid {{ display: grid; grid-template-columns: 1fr; gap: 1rem; }}
            @media (min-width: 980px) {{ .grid {{ grid-template-columns: 1fr 1fr; }} }}
            .card {{ background: #fff; border: 1px solid var(--border); border-radius: 12px; padding: 1rem; }}
            h1 {{ margin: 0 0 0.5rem 0; }}
            h2 {{ margin-top: 2rem; }}
            h3 {{ margin: 0 0 0.75rem 0; }}
            .kv code {{ background: var(--card); padding: 0.1rem 0.35rem; border-radius: 6px; }}
            details {{ border: 1px dashed var(--border); border-radius: 10px; padding: 0.75rem 0.9rem; background: #fff; }}
            summary {{ cursor: pointer; font-weight: 600; }}
            ul {{ margin-top: 0.5rem; }}
    </style>
  </head>
  <body>
        <h1>stock_data 数据服务</h1>
        <p class=\"muted\">本服务提供对本地 DuckDB + Parquet 数据仓库的只读 HTTP 访问（适合回测/研究/可视化）。</p>

        <div class=\"card kv\">
            <div><b>Base URL</b>：<code>{base_url}</code></div>
            <div class=\"muted\">提示：下面所有示例都可直接复制粘贴；把 host/port 替换成你的即可。</div>
        </div>

        <h2>交互式文档</h2>
    <ul>
      <li><a href=\"/docs\">Swagger UI</a></li>
      <li><a href=\"/redoc\">ReDoc</a></li>
      <li><a href=\"/openapi.json\">OpenAPI JSON</a></li>
    </ul>

        <h2>接口一览（含“全部参数”示例）</h2>
        <div class=\"grid\">

            <div class="card">
                <h3><code>GET /us/*</code> 美股数据</h3>
                <div class="muted">支持：<code>/us</code>, <code>/us/basic</code>, <code>/us/tradecal</code>, <code>/us/daily</code>。其中 <code>/us/daily</code> 必填 <code>ts_code</code>（如 AAPL）。</div>

                <details open>
                    <summary>1) 数据集列表</summary>
                    <pre>curl -s '{base_url}us'</pre>
                </details>

                <details>
                    <summary>2) 美股列表（us_basic）</summary>
                    <pre>curl -G '{base_url}us/basic' \
    --data-urlencode 'classify=EQ' \
    --data-urlencode 'limit=50' \
    --data-urlencode 'columns=ts_code,name,enname,classify,list_date'</pre>
                </details>

                <details>
                    <summary>3) 交易日历（us_tradecal）</summary>
                    <pre>curl -G '{base_url}us/tradecal' \
    --data-urlencode 'start_date=20260101' \
    --data-urlencode 'end_date=20260207' \
    --data-urlencode 'is_open=1' \
    --data-urlencode 'limit=20'</pre>
                </details>

                <details>
                    <summary>4) 日线行情（us_daily）</summary>
                    <pre>curl -G '{base_url}us/daily' \
    --data-urlencode 'ts_code=AAPL' \
    --data-urlencode 'start_date=20260101' \
    --data-urlencode 'end_date=20260207' \
    --data-urlencode 'order_by=trade_date desc' \
    --data-urlencode 'limit=200'</pre>
                </details>
            </div>
            <div class=\"card\">
                <h3><code>GET /health</code> 健康检查</h3>
                <div class=\"muted\">无参数；返回服务状态与本地 store 路径信息。</div>
                <pre>curl -s '{base_url}health'</pre>
            </div>

            <div class=\"card\">
                <h3><code>GET /datasets</code> 数据集列表</h3>
                <div class=\"muted\">参数：<code>lang</code> = both/en/zh（默认 both）。</div>
                <pre># 全参数示例
curl -s '{base_url}datasets?lang=both'</pre>
            </div>

            <div class=\"card\">
                <h3><code>GET /resolve</code> 代码解析</h3>
                <div class=\"muted\">把 <code>300888</code> 或 <code>300888.SZ</code> 解析为规范 <code>ts_code</code>。</div>
                <pre># 全参数示例（唯一参数）
curl -s '{base_url}resolve?symbol_or_ts_code=300888.SZ'</pre>
            </div>

            <div class=\"card\">
                <h3><code>GET /query</code> 通用查询（JSON/CSV）</h3>
                <div class=\"muted\">必填：<code>dataset</code>。可选：<code>ts_code</code>, <code>symbol</code>, <code>start_date</code>, <code>end_date</code>, <code>exchange</code>, <code>where</code>(JSON), <code>columns</code>, <code>order_by</code>, <code>limit</code>, <code>format</code>, <code>cache</code>。<br/>注意：对 <code>index_daily</code>/<code>fund_nav</code>/<code>fund_share</code>/<code>fund_div</code>/<code>dividend</code>/<code>fina_audit</code> 这类按 <code>ts_code</code> 分文件的数据集，必须提供 <code>ts_code</code>（否则无法定位文件）。</div>

                <details open>
                    <summary>1) 全参数（JSON 返回）</summary>
                    <pre>curl -G '{base_url}query' \
    --data-urlencode 'dataset=daily' \
    --data-urlencode 'ts_code=300888.SZ' \
    --data-urlencode 'symbol=300888' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240131' \
    --data-urlencode 'exchange=SSE' \
    --data-urlencode 'where={{"trade_date":["20240105","20240108"]}}' \
    --data-urlencode 'columns=ts_code,trade_date,open,high,low,close,vol' \
    --data-urlencode 'order_by=trade_date desc' \
    --data-urlencode 'limit=200' \
    --data-urlencode 'format=json' \
    --data-urlencode 'cache=true'</pre>
                </details>

                <details>
                    <summary>2) 全参数（CSV 返回，保存到文件）</summary>
                    <pre>curl -G '{base_url}query' \
    --data-urlencode 'dataset=daily' \
    --data-urlencode 'ts_code=300888.SZ' \
    --data-urlencode 'symbol=300888' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240131' \
    --data-urlencode 'exchange=SSE' \
    --data-urlencode 'where={{"trade_date":"20240105"}}' \
    --data-urlencode 'columns=ts_code,trade_date,open,high,low,close,vol' \
    --data-urlencode 'order_by=trade_date' \
    --data-urlencode 'limit=200' \
    --data-urlencode 'format=csv' \
    --data-urlencode 'cache=true' \
    -o query.csv</pre>
                </details>

                <details>
                    <summary>3) 指数日线（index_daily，按 ts_code 文件）</summary>
                    <pre>curl -G '{base_url}query' \
    --data-urlencode 'dataset=index_daily' \
    --data-urlencode 'ts_code=000001.SH' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240110' \
    --data-urlencode 'columns=ts_code,trade_date,close,pct_chg' \
    --data-urlencode 'order_by=trade_date' \
    --data-urlencode 'limit=50'</pre>
                </details>

                <details>
                    <summary>4) ETF净值（fund_nav，按 ts_code 文件）</summary>
                    <pre>curl -G '{base_url}query' \
    --data-urlencode 'dataset=fund_nav' \
    --data-urlencode 'ts_code=510300.SH' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240110' \
    --data-urlencode 'order_by=nav_date desc' \
    --data-urlencode 'limit=50'</pre>
                </details>
            </div>

            <div class=\"card\">
                <h3><code>GET /daily_adj</code> 复权行情（daily ⨝ adj_factor）</h3>
                <div class=\"muted\">必填：<code>ts_code</code>。可选：<code>start_date</code>, <code>end_date</code>, <code>how</code>(qfq/hfq/both), <code>exchange</code>, <code>limit</code>, <code>format</code>, <code>cache</code>。</div>

                <details open>
                    <summary>1) 全参数（JSON）</summary>
                    <pre>curl -G '{base_url}daily_adj' \
    --data-urlencode 'ts_code=300888.SZ' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240131' \
    --data-urlencode 'how=both' \
    --data-urlencode 'exchange=SSE' \
    --data-urlencode 'limit=200' \
    --data-urlencode 'format=json' \
    --data-urlencode 'cache=true'</pre>
                </details>

                <details>
                    <summary>2) 全参数（CSV）</summary>
                    <pre>curl -G '{base_url}daily_adj' \
    --data-urlencode 'ts_code=300888.SZ' \
    --data-urlencode 'start_date=20240101' \
    --data-urlencode 'end_date=20240131' \
    --data-urlencode 'how=both' \
    --data-urlencode 'exchange=SSE' \
    --data-urlencode 'limit=200' \
    --data-urlencode 'format=csv' \
    --data-urlencode 'cache=true' \
    -o daily_adj.csv</pre>
                </details>
            </div>

            <div class=\"card\">
                <h3><code>GET /macro/&lt;dataset&gt;</code> 宏观数据（快照）</h3>
                <div class=\"muted\">支持：<code>lpr</code>, <code>cpi</code>, <code>cn_sf</code>, <code>cn_m</code>。可选：<code>where</code>(JSON), <code>columns</code>, <code>order_by</code>, <code>limit</code>, <code>format</code>, <code>cache</code>。</div>
                <details open>
                    <summary>全参数示例（LPR）</summary>
                    <pre>curl -G '{base_url}macro/lpr' \
    --data-urlencode 'where={{"date":"20260102"}}' \
    --data-urlencode 'columns=date,1y,5y' \
    --data-urlencode 'order_by=date desc' \
    --data-urlencode 'limit=50' \
    --data-urlencode 'format=json' \
    --data-urlencode 'cache=true'</pre>
                </details>
            </div>
        </div>

        <h2>数据集（Dataset）</h2>
    <ul>
      {datasets}
    </ul>

    <h2>限制（Limit）</h2>
    <p>默认 <code>limit={settings.default_limit}</code>；最大 <code>limit={settings.max_limit}</code>（用于防止误操作拉爆内存）。</p>

        <h2>配置（环境变量）</h2>
    <ul>
      <li><code>STOCK_DATA_STORE_DIR</code> (default: <code>store</code>)</li>
      <li><code>STOCK_DATA_API_DEFAULT_LIMIT</code>, <code>STOCK_DATA_API_MAX_LIMIT</code></li>
      <li><code>STOCK_DATA_API_CACHE</code>, <code>STOCK_DATA_CACHE_BUDGET_BYTES</code>, <code>STOCK_DATA_CACHE_TTL_SECONDS</code></li>
    </ul>

        <details>
            <summary>常见问题</summary>
            <ul>
                <li><b>where 怎么写？</b> 传一个 JSON 对象字符串（建议用 <code>--data-urlencode</code>）。值支持标量或标量数组，例如：<code>{{"trade_date":["20240105","20240108"]}}</code>。</li>
                <li><b>ts_code vs symbol？</b> 推荐用 <code>ts_code</code>（如 <code>300888.SZ</code>）。不确定时先调用 <code>/resolve</code>。</li>
                <li><b>为什么返回变少？</b> 可能被 <code>limit</code> 截断；或数据本身在本地 store 中不存在。</li>
            </ul>
        </details>
  </body>
</html>"""


def create_app(*, settings: WebSettings | None = None) -> FastAPI:
    settings = settings or WebSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = open_store(
            store_dir=settings.store_dir,
            cache=settings.cache_enabled,
            cache_budget_bytes=settings.cache_budget_bytes,
            cache_ttl_seconds=settings.cache_ttl_seconds,
            exchange_default=settings.exchange_default,
        )
        app.state.store = store
        try:
            yield
        finally:
            try:
                store.close()
            except Exception:
                pass

    app = FastAPI(
        title="stock_data API",
        version="0.1.0",
        description="Read-only HTTP API for the local DuckDB + Parquet data store.",
        lifespan=lifespan,
    )

    @app.exception_handler(ValueError)
    async def _value_error_handler(_req: Request, exc: ValueError):
        return JSONResponse(status_code=400, content={"error": str(exc)})

    @app.exception_handler(FileNotFoundError)
    async def _file_not_found_handler(_req: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"error": str(exc)})

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index(request: Request):
        return HTMLResponse(_html_index(settings, base_url=str(request.base_url)))

    @app.get("/health")
    async def health():
        store: StockStore = app.state.store
        duckdb_exists = os.path.exists(store.duckdb_path)
        parquet_exists = os.path.isdir(store.parquet_dir)
        return {
            "status": "ok",
            "store_dir": store.store_dir,
            "duckdb_path": store.duckdb_path,
            "duckdb_exists": duckdb_exists,
            "parquet_dir": store.parquet_dir,
            "parquet_exists": parquet_exists,
            "time": int(time.time()),
        }

    @app.get("/stat")
    async def stat(datasets: str = Query("all", description="Comma-separated dataset names or 'all'")):
        cfg = RunConfig(store_dir=settings.store_dir, rpm=500, workers=12)
        datasets_list = fetch_stats_json(cfg, datasets=datasets)
        return {
            "datasets": datasets_list,
            "count": len(datasets_list),
            "generated_at": int(time.time()),
        }

    @app.get("/datasets")
    async def datasets(lang: Literal["both", "en", "zh"] = "both"):
        out: list[dict[str, Any]] = []
        for d in DATASETS:
            item = {
                "name": d.name,
                "category": d.category,
                "partitioning": d.partitioning,
                "source": d.source,
            }
            if lang in {"both", "en"}:
                item["desc_en"] = d.desc_en
            if lang in {"both", "zh"}:
                item["desc_zh"] = d.desc_zh
            out.append(item)
        return {"datasets": out, "count": len(out)}

    @app.get("/resolve")
    async def resolve(symbol_or_ts_code: str = Query(..., description="e.g. 300888 or 300888.SZ")):
        store: StockStore = app.state.store
        r = store.resolve(symbol_or_ts_code)
        return {"symbol": r.symbol, "ts_code": r.ts_code, "list_date": r.list_date}

    @app.get("/query")
    async def query(
        dataset: str = Query(..., description=f"One of: {', '.join(ALL_DATASET_NAMES)}"),
        ts_code: str | None = Query(None, description="Filter: ts_code"),
        symbol: str | None = Query(None, description="Filter: symbol"),
        start_date: str | None = Query(None, description="YYYYMMDD (for trade_date partitioned datasets)"),
        end_date: str | None = Query(None, description="YYYYMMDD (for trade_date partitioned datasets)"),
        exchange: str | None = Query(None, description="Exchange (used by trade_cal and some partitioned datasets)"),
        where: str | None = Query(None, description="Additional filters as JSON object, e.g. {\"trade_date\":\"20240105\"}"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. trade_date or trade_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json", description="Response format"),
        cache: bool = Query(True, description="Use store in-memory cache"),
    ):
        store: StockStore = app.state.store

        ds = (dataset or "").strip()
        if ds not in set(ALL_DATASET_NAMES) | {"trade_cal"}:
            # trade_cal is in ALL_DATASET_NAMES today, but keep this tolerant.
            raise ValueError(f"Unknown dataset: {ds}")

        where_obj = _parse_where_json(where)
        where_obj = dict(where_obj or {})
        if ts_code:
            where_obj["ts_code"] = ts_code
        if symbol:
            where_obj["symbol"] = symbol

        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)

        df = store.read(
            ds,
            where=where_obj or None,
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            exchange=exchange,
            cache=cache,
        )

        if format == "csv":
            csv = df.to_csv(index=False)
            return Response(content=csv, media_type="text/csv; charset=utf-8")

        return {
            "dataset": ds,
            "rows": int(len(df)),
            "data": _df_to_json_records(df),
        }

    # -----------------------------
    # Convenience APIs: index & ETF
    # -----------------------------
    @app.get("/index_basic")
    async def index_basic(
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        df = store.read("index_basic", limit=lim, cache=cache)
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "index_basic", "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/index_daily")
    async def index_daily(
        ts_code: str = Query(..., description="Index code, e.g. 000001.SH"),
        start_date: str | None = Query(None, description="YYYYMMDD"),
        end_date: str | None = Query(None, description="YYYYMMDD"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. trade_date or trade_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "index_daily",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "index_daily", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/fund_basic")
    async def fund_basic(
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        df = store.read("fund_basic", limit=lim, cache=cache)
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "fund_basic", "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/fund_nav")
    async def fund_nav(
        ts_code: str = Query(..., description="ETF code, e.g. 510300.SH"),
        start_date: str | None = Query(None, description="YYYYMMDD (nav_date)"),
        end_date: str | None = Query(None, description="YYYYMMDD (nav_date)"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. nav_date or nav_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "fund_nav",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "fund_nav", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/fund_share")
    async def fund_share(
        ts_code: str = Query(..., description="ETF code, e.g. 510300.SH"),
        start_date: str | None = Query(None, description="YYYYMMDD (trade_date)"),
        end_date: str | None = Query(None, description="YYYYMMDD (trade_date)"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. trade_date or trade_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "fund_share",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "fund_share", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/fund_div")
    async def fund_div(
        ts_code: str = Query(..., description="ETF code, e.g. 510300.SH"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "fund_div",
            where={"ts_code": ts_code},
            columns=cols,
            limit=lim,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "fund_div", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/dividend")
    async def dividend(
        ts_code: str = Query(..., description="Stock code, e.g. 000001.SZ"),
        start_date: str | None = Query(None, description="YYYYMMDD (end_date)"),
        end_date: str | None = Query(None, description="YYYYMMDD (end_date)"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. end_date or end_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "dividend",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "dividend", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/fina_audit")
    async def fina_audit(
        ts_code: str = Query(..., description="Stock code, e.g. 000001.SZ"),
        start_date: str | None = Query(None, description="YYYYMMDD (end_date)"),
        end_date: str | None = Query(None, description="YYYYMMDD (end_date)"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. end_date or end_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "fina_audit",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "fina_audit", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/daily_adj")
    async def daily_adj(
        ts_code: str = Query(..., description="e.g. 300888.SZ"),
        start_date: str | None = Query(None, description="YYYYMMDD"),
        end_date: str | None = Query(None, description="YYYYMMDD"),
        how: Literal["qfq", "hfq", "both"] = Query("both"),
        exchange: str | None = Query(None),
        limit: int | None = Query(None, description="Row limit (applied after join)"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        df = store.daily_adj(
            ts_code,
            start_date=start_date,
            end_date=end_date,
            how=how,
            exchange=exchange,
            cache=cache,
        )
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        if lim is not None:
            df = df.head(int(lim))

        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")

        return {"ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    # -----------------------------
    # Convenience APIs: macro
    # -----------------------------
    @app.get("/macro")
    async def macro_list():
        # Keep this explicit to avoid confusion with future macro expansions.
        return {
            "datasets": ["lpr", "cpi", "cn_sf", "cn_m"],
            "count": 4,
        }

    @app.get("/macro/{dataset}")
    async def macro_dataset(
        dataset: MacroDataset,
        where: str | None = Query(None, description="Additional filters as JSON object"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. date desc or month desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        where_obj = _parse_where_json(where)

        df = store.read(
            dataset,
            where=where_obj or None,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )

        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": dataset, "rows": int(len(df)), "data": _df_to_json_records(df)}

    # -----------------------------
    # Convenience APIs: US stocks
    # -----------------------------
    @app.get("/us")
    async def us_list():
        return {
            "datasets": ["us_basic", "us_tradecal", "us_daily"],
            "count": 3,
        }

    @app.get("/us/basic")
    async def us_basic(
        ts_code: str | None = Query(None, description="e.g. AAPL"),
        classify: str | None = Query(None, description="ADR/GDR/EQ"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.us_basic(ts_code=ts_code, classify=classify, columns=cols, limit=lim, cache=cache)
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "us_basic", "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/us/tradecal")
    async def us_tradecal(
        start_date: str | None = Query(None, description="YYYYMMDD"),
        end_date: str | None = Query(None, description="YYYYMMDD"),
        is_open: int | None = Query(None, description="1=open,0=closed"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        df = store.us_tradecal(start_date=start_date, end_date=end_date, is_open=is_open, cache=cache)
        cols = _parse_columns(columns)
        if cols:
            keep = [c for c in cols if c in df.columns]
            if keep:
                df = df[keep]
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        df = df.head(int(lim))
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "us_tradecal", "rows": int(len(df)), "data": _df_to_json_records(df)}

    @app.get("/us/daily")
    async def us_daily(
        ts_code: str = Query(..., description="e.g. AAPL"),
        start_date: str | None = Query(None, description="YYYYMMDD"),
        end_date: str | None = Query(None, description="YYYYMMDD"),
        columns: str | None = Query(None, description="Comma-separated column list"),
        order_by: str | None = Query(None, description="e.g. trade_date desc"),
        limit: int | None = Query(None, description="Row limit"),
        format: Literal["json", "csv"] = Query("json"),
        cache: bool = Query(True),
    ):
        store: StockStore = app.state.store
        lim = _clamp_limit(limit, default_limit=settings.default_limit, max_limit=settings.max_limit)
        cols = _parse_columns(columns)
        df = store.read(
            "us_daily",
            where={"ts_code": ts_code},
            start_date=start_date,
            end_date=end_date,
            columns=cols,
            limit=lim,
            order_by=order_by,
            cache=cache,
        )
        if format == "csv":
            return Response(content=df.to_csv(index=False), media_type="text/csv; charset=utf-8")
        return {"dataset": "us_daily", "ts_code": ts_code, "rows": int(len(df)), "data": _df_to_json_records(df)}

    def _store_root() -> Path:
        # Resolve to avoid traversal tricks.
        return Path(settings.store_dir).expanduser().resolve()

    def _safe_store_file(rel_posix_path: str) -> Path:
        rel = (rel_posix_path or "").strip().lstrip("/")
        if not rel:
            raise ValueError("path is required")

        # Only allow syncing these subtrees.
        if not (rel == "duckdb" or rel.startswith("duckdb/") or rel == "parquet" or rel.startswith("parquet/")):
            raise ValueError("path must be under duckdb/ or parquet/")

        root = _store_root()
        p = (root / Path(rel)).resolve()
        try:
            p.relative_to(root)
        except Exception as e:  # noqa: BLE001
            raise ValueError("invalid path") from e
        if not p.is_file():
            raise FileNotFoundError(rel)
        return p

    @app.get("/sync/manifest")
    async def sync_manifest(
        hash: bool = Query(False, description="Include sha256 for each file (slower)"),
    ):
        root = _store_root()
        files: list[dict[str, Any]] = []

        def _walk(base_rel: str) -> None:
            base = (root / base_rel)
            if not base.exists():
                return
            for dirpath, _dirnames, filenames in os.walk(base):
                for fn in filenames:
                    if fn == ".DS_Store" or fn.startswith("._"):
                        continue
                    p = Path(dirpath) / fn
                    try:
                        st = p.stat()
                    except FileNotFoundError:
                        continue
                    rel = p.resolve().relative_to(root).as_posix()
                    item: dict[str, Any] = {
                        "path": rel,
                        "size": int(st.st_size),
                        "mtime_ns": int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))),
                    }
                    if hash:
                        h = hashlib.sha256()
                        with p.open("rb") as f:
                            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                                h.update(chunk)
                        item["sha256"] = h.hexdigest()
                    files.append(item)

        _walk("duckdb")
        _walk("parquet")

        return {
            "store_dir": str(root),
            "generated_at": int(time.time()),
            "files": files,
            "count": len(files),
        }

    @app.get("/sync/file")
    async def sync_file(path: str = Query(..., description="Relative file path under duckdb/ or parquet/")):
        p = _safe_store_file(path)
        return FileResponse(path=str(p), media_type="application/octet-stream")

    return app


# Uvicorn-friendly global app (configuration via env vars).
app = create_app()
