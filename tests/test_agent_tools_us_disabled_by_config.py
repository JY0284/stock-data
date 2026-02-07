from __future__ import annotations

from pathlib import Path

import pytest


def _set_test_config(monkeypatch, tmp_path: Path, yaml_text: str) -> None:
    cfg_path = tmp_path / "stock_data_test_config.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("STOCK_DATA_CONFIG", str(cfg_path))
    from stock_data.config import clear_config_cache

    clear_config_cache()


def test_agent_tools_us_not_exposed_when_disabled(tmp_path: Path, monkeypatch) -> None:
    _set_test_config(
        monkeypatch,
        tmp_path,
        """
categories:
    us: false
    us_index: false
""".lstrip(),
    )

    from stock_data.agent_tools import clear_store_cache, get_us_basic, get_us_basic_detail, get_us_tradecal, get_us_daily_prices

    # Ensure any cached store/config is cleared
    clear_store_cache()

    with pytest.raises(ValueError):
        get_us_basic(store_dir=str(tmp_path / "store"))

    with pytest.raises(ValueError):
        get_us_basic_detail("AAPL", store_dir=str(tmp_path / "store"))

    with pytest.raises(ValueError):
        get_us_tradecal(store_dir=str(tmp_path / "store"))

    with pytest.raises(ValueError):
        get_us_daily_prices("AAPL", store_dir=str(tmp_path / "store"))
