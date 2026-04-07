from golden_cross.symbols import normalize_interval, normalize_symbol


def test_normalize_symbol_supports_notebook_aliases() -> None:
    assert normalize_symbol("btcusd") == "BTC-USD"
    assert normalize_symbol("spy.us") == "SPY"
    assert normalize_symbol("^spx") == "^GSPC"
    assert normalize_symbol("brk.b.us") == "BRK-B"


def test_normalize_interval_supports_notebook_daily_alias() -> None:
    assert normalize_interval("d") == "1d"
    assert normalize_interval("1d") == "1d"
