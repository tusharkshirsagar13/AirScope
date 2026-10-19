"""Shared pytest fixtures.

All test data here is SYNTHETIC, written to temporary folders at test time.
It is never mixed with the real dataset in data/raw/.
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_mapping, load_settings  # noqa: E402
from src.data_loader import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    settings = load_settings()
    settings["spark"]["shuffle_partitions"] = 2
    s = get_spark(settings)
    yield s
    s.stop()


@pytest.fixture()
def settings(tmp_path):
    s = copy.deepcopy(load_settings())
    s["paths"]["processed_dir"] = str(tmp_path / "processed")
    s["paths"]["reports_dir"] = str(tmp_path / "reports")
    return s


@pytest.fixture(scope="session")
def mapping():
    return load_mapping()


@pytest.fixture()
def write(tmp_path):
    """write("name.csv", "text") -> Path of a synthetic file in a temp dir."""
    def _w(name: str, text: str) -> Path:
        p = tmp_path / "raw" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        return p
    return _w


# A small file in the same layout as the UCI Air Quality CSV:
# ';' delimiter, ',' decimals, -200 = missing, trailing empty columns.
UCI_LIKE = (
    "Date;Time;CO(GT);NO2(GT);T;RH;;\n"
    "10/03/2004;18.00.00;2,6;113;13,6;48,9;;\n"
    "10/03/2004;19.00.00;2;92;-1,5;47,7;;\n"
    "10/03/2004;20.00.00;-200;-200;11,9;54,0;;\n"
    "10/03/2004;21.00.00;abc;122;11,0;60,0;;\n"
    "10/03/2004;22.00.00;80;131;11,2;150;;\n"          # CO=80 > 50 and RH=150 > 100 -> invalid
    "10/03/2004;22.00.00;80;131;11,2;150;;\n"          # exact duplicate
    "13/03/2004;23.00.00;-200;-200;-200;-200;;\n"      # no valid measurement
    "99/99/2004;10.00.00;1,0;50;10,0;50,0;;\n"         # bad timestamp
    ";;;;;;;\n"                                         # blank row
)


@pytest.fixture()
def uci_like(write):
    return write("uci_like.csv", UCI_LIKE)
