"""Shared pytest setup.

The pipeline fetches funding rate / open interest from Hyperliquid's public
REST API (`live/derivs.py`). Tests must never touch the network, so that step is
switched off for every test by default; the tests that exercise it
(`tests/test_derivs.py`, one integration test in `tests/test_live_incremental.py`)
remove the variable and inject fake fetchers instead.
"""

import pytest


@pytest.fixture(autouse=True)
def _no_derivs_network(monkeypatch):
    monkeypatch.setenv("HYDRA_DERIVS_DISABLED", "1")
