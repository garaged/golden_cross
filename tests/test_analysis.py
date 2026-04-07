import pandas as pd

from golden_cross.analysis import compute_ma_report
from golden_cross.models import MAConfig


def test_compute_ma_report_detects_regime_and_events() -> None:
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "Close": [10, 9, 8, 7, 8, 9, 10, 11, 12, 13],
            "Open": [10, 9, 8, 7, 8, 9, 10, 11, 12, 13],
            "High": [10, 9, 8, 7, 8, 9, 10, 11, 12, 13],
            "Low": [10, 9, 8, 7, 8, 9, 10, 11, 12, 13],
            "Volume": [1] * 10,
        }
    )
    cfg = MAConfig(short=2, long=3)

    report = compute_ma_report(df=df, symbol="TEST", interval="1d", cfg=cfg)

    assert report.latest["regime"] in {"BULLISH", "BEARISH"}
    assert set(report.events["type"].unique()).issubset({"GOLDEN_CROSS", "DEATH_CROSS"})
