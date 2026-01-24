from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Iterable


def parse_yyyymmdd(s: str) -> _dt.date:
    return _dt.datetime.strptime(s, "%Y%m%d").date()


def fmt_yyyymmdd(d: _dt.date) -> str:
    return d.strftime("%Y%m%d")


def year_windows(start_date: str, end_date: str) -> list[tuple[str, str, str]]:
    """
    Returns list of (year, window_start_yyyymmdd, window_end_yyyymmdd), inclusive.
    """
    start = parse_yyyymmdd(start_date)
    end = parse_yyyymmdd(end_date)
    out: list[tuple[str, str, str]] = []
    for y in range(start.year, end.year + 1):
        ws = _dt.date(y, 1, 1)
        we = _dt.date(y, 12, 31)
        if y == start.year:
            ws = start
        if y == end.year:
            we = end
        out.append((str(y), fmt_yyyymmdd(ws), fmt_yyyymmdd(we)))
    return out


def filter_between(dates: Iterable[str], start_date: str, end_date: str) -> list[str]:
    s = parse_yyyymmdd(start_date)
    e = parse_yyyymmdd(end_date)
    out = []
    for d in dates:
        dd = parse_yyyymmdd(d)
        if s <= dd <= e:
            out.append(d)
    return out


def unique_preserve(xs: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

