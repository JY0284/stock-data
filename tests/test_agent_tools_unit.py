import pandas as pd

from stock_data.agent_tools import _clean_row, _df_to_payload, _single_row_payload


def test_clean_row_removes_none_and_nan() -> None:
    row = {"a": 1, "b": None, "c": float("nan"), "d": "x"}
    cleaned = _clean_row(row)
    assert cleaned == {"a": 1, "d": "x"}


def test_df_to_payload_pagination_and_metadata() -> None:
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5], "y": ["a", "b", "c", "d", "e"]})

    out = _df_to_payload(df, offset=1, limit=2, compact=True)
    assert out["total_count"] == 5
    assert len(out["rows"]) == 2
    assert out["rows"][0]["x"] == 2
    assert out["showing"] == "2-3"
    assert out["has_more"] is True

    out2 = _df_to_payload(df, offset=10, limit=2)
    assert out2["rows"] == []
    assert out2["showing"] == "0-0"
    assert out2["has_more"] is False


def test_single_row_payload_found_and_not_found() -> None:
    empty = pd.DataFrame(columns=["a"])  # empty
    out0 = _single_row_payload(empty)
    assert out0 == {"found": False, "data": None}

    df = pd.DataFrame([{"a": 1, "b": None, "c": float("nan")}])
    out1 = _single_row_payload(df, compact=True)
    assert out1["found"] is True
    assert out1["data"]["a"] == 1
    assert "b" not in out1["data"]
    assert "c" not in out1["data"]
