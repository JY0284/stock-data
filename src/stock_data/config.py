"""Configuration module for stock_data.

This module provides a centralized configuration system that controls:
- Which datasets to skip in CLI operations (update/backfill/stat)
- Whether datasets are exposed in the web service
- Whether datasets are exposed in agent tools

Configuration is loaded from a YAML file (default: stock_data.yaml in the current directory
or in the store directory).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DatasetConfig:
    """Configuration for a specific dataset or category."""

    # If True, skip this dataset in CLI update/backfill operations
    skip_ingestion: bool = False
    # If True, skip this dataset in CLI stat command
    skip_stat: bool = False
    # If True, expose this dataset in the web service
    enable_web: bool = True
    # If True, expose this dataset in agent tools
    enable_agent: bool = True


@dataclass
class AppConfig:
    """Application-wide configuration."""

    # Global settings
    store_dir: str = "store"
    rpm: int = 500
    workers: int = 12

    # Web service settings
    web_host: str = "0.0.0.0"
    web_port: int = 8000

    # Per-dataset configuration (dataset_name -> DatasetConfig)
    # If a dataset is not listed, defaults apply (all enabled, nothing skipped)
    datasets: dict[str, DatasetConfig] = field(default_factory=dict)

    # Category-level configuration (category_name -> DatasetConfig)
    # Categories: basic, market, finance, etf, macro, us, us_index
    # Dataset-level config overrides category-level config
    categories: dict[str, DatasetConfig] = field(default_factory=dict)

    def get_dataset_config(self, dataset_name: str, category: str | None = None) -> DatasetConfig:
        """Get effective configuration for a dataset.

        Priority: dataset-specific > category-level > default
        """
        # Check dataset-specific config first
        if dataset_name in self.datasets:
            return self.datasets[dataset_name]

        # Then check category-level config
        if category and category in self.categories:
            return self.categories[category]

        # Return default config
        return DatasetConfig()

    def should_skip_ingestion(self, dataset_name: str, category: str | None = None) -> bool:
        """Check if a dataset should be skipped during ingestion (update/backfill)."""
        return self.get_dataset_config(dataset_name, category).skip_ingestion

    def should_skip_stat(self, dataset_name: str, category: str | None = None) -> bool:
        """Check if a dataset should be skipped in stat command."""
        return self.get_dataset_config(dataset_name, category).skip_stat

    def is_web_enabled(self, dataset_name: str, category: str | None = None) -> bool:
        """Check if a dataset should be exposed in web service."""
        return self.get_dataset_config(dataset_name, category).enable_web

    def is_agent_enabled(self, dataset_name: str, category: str | None = None) -> bool:
        """Check if a dataset should be exposed in agent tools."""
        return self.get_dataset_config(dataset_name, category).enable_agent

    def filter_datasets_for_ingestion(self, datasets: list[str], dataset_categories: dict[str, str] | None = None) -> list[str]:
        """Filter out datasets that should be skipped during ingestion."""
        dataset_categories = dataset_categories or {}
        return [
            d for d in datasets
            if not self.should_skip_ingestion(d, dataset_categories.get(d))
        ]

    def filter_datasets_for_stat(self, datasets: list[str], dataset_categories: dict[str, str] | None = None) -> list[str]:
        """Filter out datasets that should be skipped in stat command."""
        dataset_categories = dataset_categories or {}
        return [
            d for d in datasets
            if not self.should_skip_stat(d, dataset_categories.get(d))
        ]

    def filter_datasets_for_web(self, datasets: list[str], dataset_categories: dict[str, str] | None = None) -> list[str]:
        """Filter datasets for web service exposure."""
        dataset_categories = dataset_categories or {}
        return [
            d for d in datasets
            if self.is_web_enabled(d, dataset_categories.get(d))
        ]

    def filter_datasets_for_agent(self, datasets: list[str], dataset_categories: dict[str, str] | None = None) -> list[str]:
        """Filter datasets for agent tools exposure."""
        dataset_categories = dataset_categories or {}
        return [
            d for d in datasets
            if self.is_agent_enabled(d, dataset_categories.get(d))
        ]


def _parse_dataset_config(data: dict[str, Any] | bool | None) -> DatasetConfig:
    """Parse a dataset/category config from YAML data."""
    if data is None:
        return DatasetConfig()

    if isinstance(data, bool):
        # Shorthand: false means skip everything, true means enable everything
        if data:
            return DatasetConfig()
        else:
            return DatasetConfig(
                skip_ingestion=True,
                skip_stat=True,
                enable_web=False,
                enable_agent=False,
            )

    return DatasetConfig(
        skip_ingestion=data.get("skip_ingestion", False),
        skip_stat=data.get("skip_stat", False),
        enable_web=data.get("enable_web", True),
        enable_agent=data.get("enable_agent", True),
    )


def load_config(config_path: str | Path | None = None, store_dir: str | None = None) -> AppConfig:
    """Load configuration from a YAML file.

    Search order:
    1. Explicit config_path if provided
    2. STOCK_DATA_CONFIG env var
    3. stock_data.yaml in current directory
    4. stock_data.yaml in store directory (if provided)
    5. Default config (all datasets enabled)

    Args:
        config_path: Explicit path to config file
        store_dir: Store directory (used as fallback search location)

    Returns:
        Loaded AppConfig instance
    """
    # Determine config file path
    # If an explicit config_path is provided, treat it as authoritative.
    # This avoids surprising fallbacks to repo-level defaults when callers
    # intentionally point at a config that doesn't exist (common in tests).
    if config_path:
        p = Path(config_path)
        if not p.exists():
            return AppConfig(store_dir=store_dir or "store")
        search_paths: list[Path] = [p]
    else:
        search_paths = []

    env_path = os.environ.get("STOCK_DATA_CONFIG")
    if env_path:
        search_paths.append(Path(env_path))

    search_paths.append(Path("stock_data.yaml"))
    search_paths.append(Path("stock_data.yml"))

    if store_dir:
        search_paths.append(Path(store_dir) / "stock_data.yaml")
        search_paths.append(Path(store_dir) / "stock_data.yml")

    # Find first existing config file
    config_file: Path | None = None
    for p in search_paths:
        if p.exists():
            config_file = p
            break

    if config_file is None:
        # Return default config
        return AppConfig(store_dir=store_dir or "store")

    # Load and parse YAML
    with open(config_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return _parse_config_data(data, default_store_dir=store_dir)


def _parse_config_data(data: dict[str, Any], default_store_dir: str | None = None) -> AppConfig:
    """Parse configuration data from a dict (usually loaded from YAML)."""
    # Parse global settings
    store_dir = data.get("store_dir", default_store_dir or "store")
    rpm = data.get("rpm", 500)
    workers = data.get("workers", 12)
    web_data = data.get("web") or {}
    web_host = web_data.get("host", "0.0.0.0")
    web_port = web_data.get("port", 8000)

    # Parse category configs
    categories: dict[str, DatasetConfig] = {}
    categories_data = data.get("categories") or {}
    for cat_name, cat_data in categories_data.items():
        categories[cat_name] = _parse_dataset_config(cat_data)

    # Parse dataset configs
    datasets: dict[str, DatasetConfig] = {}
    datasets_data = data.get("datasets") or {}
    for ds_name, ds_data in datasets_data.items():
        datasets[ds_name] = _parse_dataset_config(ds_data)

    return AppConfig(
        store_dir=store_dir,
        rpm=rpm,
        workers=workers,
        web_host=web_host,
        web_port=web_port,
        datasets=datasets,
        categories=categories,
    )


# Global config cache
_config_cache: AppConfig | None = None


def get_config(store_dir: str | None = None, reload: bool = False) -> AppConfig:
    """Get the global configuration instance.

    This function caches the config for performance. Use reload=True to force
    re-reading from disk.

    Args:
        store_dir: Store directory (used for config search if not already loaded)
        reload: If True, reload config from disk even if cached

    Returns:
        AppConfig instance
    """
    global _config_cache

    if _config_cache is None or reload:
        _config_cache = load_config(store_dir=store_dir)

    return _config_cache


def clear_config_cache() -> None:
    """Clear the cached configuration. Useful for tests."""
    global _config_cache
    _config_cache = None


# Helper to get dataset categories from datasets.py
def get_dataset_categories() -> dict[str, str]:
    """Get a mapping of dataset name -> category from DATASETS."""
    from stock_data.datasets import DATASETS

    return {d.name: d.category for d in DATASETS}
