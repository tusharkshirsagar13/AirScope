"""Configuration loading and project paths.

Every path in the project is resolved relative to PROJECT_ROOT so the app runs
from any working directory and on any operating system.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
SETTINGS_FILE = CONFIG_DIR / "settings.yaml"
MAPPING_FILE = CONFIG_DIR / "column_mapping.yaml"


class ConfigError(Exception):
    """Raised when a configuration file is missing or malformed."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"Could not parse {path.name}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name} must contain a YAML mapping at the top level.")
    return data


def load_settings(path: Path | None = None) -> dict[str, Any]:
    return _read_yaml(path or SETTINGS_FILE)


def load_mapping(path: Path | None = None) -> dict[str, Any]:
    data = _read_yaml(path or MAPPING_FILE)
    data.setdefault("roles", {})
    data.setdefault("measurements", {})
    return data


def resolve_path(p: str | Path) -> Path:
    """Resolve a (possibly relative) path against the project root."""
    p = Path(p)
    return p if p.is_absolute() else (PROJECT_ROOT / p)


def config_fingerprint(*objs: Any) -> str:
    """Stable hash of configuration objects (used to invalidate cached outputs)."""
    blob = json.dumps([copy.deepcopy(o) for o in objs], sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
