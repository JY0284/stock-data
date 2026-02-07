"""Tests for the config module."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from stock_data.config import (
    AppConfig,
    DatasetConfig,
    load_config,
    _parse_config_data,
    get_config,
    clear_config_cache,
)


@pytest.fixture(autouse=True)
def clean_config_cache():
    """Clean config cache before and after each test."""
    clear_config_cache()
    yield
    clear_config_cache()


def test_default_config():
    """Test default config when no file exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = load_config(config_path=Path(tmpdir) / "nonexistent.yaml")
        assert cfg.store_dir == "store"
        assert cfg.rpm == 500
        assert cfg.workers == 12
        assert cfg.datasets == {}
        assert cfg.categories == {}


def test_parse_empty_config():
    """Test parsing empty config data."""
    cfg = _parse_config_data({})
    assert cfg.store_dir == "store"
    assert cfg.rpm == 500


def test_parse_full_config():
    """Test parsing a full config with datasets and categories."""
    data = {
        "store_dir": "/custom/store",
        "rpm": 1000,
        "workers": 8,
        "web": {"host": "127.0.0.1", "port": 9000},
        "categories": {
            "us": {
                "skip_ingestion": True,
                "enable_web": False,
            }
        },
        "datasets": {
            "us_daily": {
                "skip_ingestion": True,
                "skip_stat": True,
                "enable_web": False,
                "enable_agent": False,
            }
        },
    }
    cfg = _parse_config_data(data)
    assert cfg.store_dir == "/custom/store"
    assert cfg.rpm == 1000
    assert cfg.workers == 8
    assert cfg.web_host == "127.0.0.1"
    assert cfg.web_port == 9000
    
    assert "us" in cfg.categories
    assert cfg.categories["us"].skip_ingestion is True
    assert cfg.categories["us"].enable_web is False
    
    assert "us_daily" in cfg.datasets
    assert cfg.datasets["us_daily"].skip_ingestion is True
    assert cfg.datasets["us_daily"].enable_agent is False


def test_parse_shorthand_false():
    """Test that `dataset: false` disables everything."""
    data = {"datasets": {"us_daily": False}}
    cfg = _parse_config_data(data)
    ds_cfg = cfg.datasets["us_daily"]
    assert ds_cfg.skip_ingestion is True
    assert ds_cfg.skip_stat is True
    assert ds_cfg.enable_web is False
    assert ds_cfg.enable_agent is False


def test_parse_shorthand_true():
    """Test that `dataset: true` enables everything (default)."""
    data = {"datasets": {"us_daily": True}}
    cfg = _parse_config_data(data)
    ds_cfg = cfg.datasets["us_daily"]
    assert ds_cfg.skip_ingestion is False
    assert ds_cfg.skip_stat is False
    assert ds_cfg.enable_web is True
    assert ds_cfg.enable_agent is True


def test_get_dataset_config_priority():
    """Test that dataset-specific config overrides category-level config."""
    cfg = AppConfig(
        categories={
            "us": DatasetConfig(skip_ingestion=True, enable_web=False),
        },
        datasets={
            "us_daily": DatasetConfig(skip_ingestion=False, enable_web=True),
        },
    )
    
    # us_daily has specific config - should override category
    assert cfg.should_skip_ingestion("us_daily", "us") is False
    assert cfg.is_web_enabled("us_daily", "us") is True
    
    # us_basic uses category-level config
    assert cfg.should_skip_ingestion("us_basic", "us") is True
    assert cfg.is_web_enabled("us_basic", "us") is False
    
    # unknown dataset with unknown category - uses default
    assert cfg.should_skip_ingestion("other", None) is False
    assert cfg.is_web_enabled("other", None) is True


def test_filter_datasets_for_ingestion():
    """Test filtering datasets for ingestion."""
    cfg = AppConfig(
        datasets={
            "us_daily": DatasetConfig(skip_ingestion=True),
        }
    )
    datasets = ["daily", "us_daily", "stock_basic"]
    filtered = cfg.filter_datasets_for_ingestion(datasets)
    assert "daily" in filtered
    assert "stock_basic" in filtered
    assert "us_daily" not in filtered


def test_filter_datasets_for_web():
    """Test filtering datasets for web access."""
    cfg = AppConfig(
        datasets={
            "us_daily": DatasetConfig(enable_web=False),
        }
    )
    datasets = ["daily", "us_daily", "stock_basic"]
    filtered = cfg.filter_datasets_for_web(datasets)
    assert "daily" in filtered
    assert "stock_basic" in filtered
    assert "us_daily" not in filtered


def test_filter_datasets_for_agent():
    """Test filtering datasets for agent tools."""
    cfg = AppConfig(
        datasets={
            "us_daily": DatasetConfig(enable_agent=False),
        }
    )
    datasets = ["daily", "us_daily", "stock_basic"]
    filtered = cfg.filter_datasets_for_agent(datasets)
    assert "daily" in filtered
    assert "stock_basic" in filtered
    assert "us_daily" not in filtered


def test_load_config_from_yaml_file():
    """Test loading config from an actual YAML file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_content = """
store_dir: /my/store
rpm: 300
datasets:
  us_daily:
    skip_ingestion: true
"""
        yaml_path = Path(tmpdir) / "stock_data.yaml"
        yaml_path.write_text(yaml_content)
        
        cfg = load_config(config_path=yaml_path)
        assert cfg.store_dir == "/my/store"
        assert cfg.rpm == 300
        assert cfg.should_skip_ingestion("us_daily") is True


def test_get_config_caching():
    """Test that get_config returns cached config."""
    cfg1 = get_config()
    cfg2 = get_config()
    assert cfg1 is cfg2
    
    cfg3 = get_config(reload=True)
    # After reload, it should be a fresh instance
    # (but may be equivalent if no config file changed)
    assert cfg3 is not cfg1
