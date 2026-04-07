from __future__ import annotations

import argparse
from pathlib import Path

from golden_cross.api import ma_cross_report
from golden_cross.models import MAConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="golden-cross")
    subparsers = parser.add_subparsers(dest="command", required=True)

    report = subparsers.add_parser("report", help="Fetch OHLCV, compute MA crossover report, and optionally plot it")
    report.add_argument("symbol", help="Symbol in notebook style or Yahoo style, e.g. btcusd, spy.us, BTC-USD")
    report.add_argument("--interval", default="1d", help="Interval such as 1d, 1wk, 1mo, 1h")
    report.add_argument("--start", default=None, help="Inclusive start date, e.g. 2020-01-01")
    report.add_argument("--end", default=None, help="Inclusive end date, e.g. 2026-04-01")
    report.add_argument("--short", type=int, default=50, help="Short SMA period")
    report.add_argument("--long", type=int, default=200, help="Long SMA period")
    report.add_argument(
        "--price-cross-ma",
        action="append",
        type=int,
        default=[],
        help="Optional SMA period to mark price crossing events against. Can be repeated.",
    )
    report.add_argument("--plot-days", type=int, default=3650, help="How many recent days to include in the plot")
    report.add_argument("--cache-dir", default=None, help="Override cache directory")
    report.add_argument("--force-refresh", action="store_true", help="Ignore cached data and fetch again")
    report.add_argument("--no-plot", action="store_true", help="Skip plotting")
    report.add_argument("--print-events", type=int, default=10, help="How many recent events to print")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "report":
        cfg = MAConfig(
            short=args.short,
            long=args.long,
            price_cross_mas=tuple(args.price_cross_ma),
            plot_days=args.plot_days,
        )
        ma_cross_report(
            symbol=args.symbol,
            interval=args.interval,
            cfg=cfg,
            plot=not args.no_plot,
            print_events=args.print_events,
            start=args.start,
            end=args.end,
            cache_dir=Path(args.cache_dir) if args.cache_dir else None,
            force_refresh=args.force_refresh,
        )
