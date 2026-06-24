"""Shared fixtures for all test modules."""

import sys
import os

import numpy as np
import pandas as pd
import pytest

# Ensure repo root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture()
def sample_ohlcv_df():
    """Return a 250-row OHLCV DataFrame with realistic synthetic data."""
    np.random.seed(42)
    n = 250
    base_price = 100.0
    # random walk
    returns = np.random.normal(0.0002, 0.01, n)
    close = base_price * np.cumprod(1 + returns)
    high = close * (1 + np.abs(np.random.normal(0, 0.005, n)))
    low = close * (1 - np.abs(np.random.normal(0, 0.005, n)))
    open_ = (close + np.roll(close, 1)) / 2
    open_[0] = base_price
    volume = np.random.uniform(1000, 50000, n)

    df = pd.DataFrame({
        "time": pd.date_range("2024-01-01", periods=n, freq="5min"),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "turnover": volume * close,
    })
    return df


@pytest.fixture()
def sample_df_with_indicators(sample_ohlcv_df):
    """Return OHLCV DataFrame with technical indicators already added."""
    from scanner_scalp import add_indicators
    return add_indicators(sample_ohlcv_df)
