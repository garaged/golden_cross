from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from golden_cross.models import MAReport


def plot_ma_report(
    report: MAReport,
    show_price: bool = True,
    show_mas: bool = True,
    mark_ma_crosses: bool = True,
    mark_price_crosses: bool = True,
) -> None:
    df = report.df
    cfg = report.cfg

    date_col = cfg.date_col
    close_col = cfg.close_col
    short_col = f"SMA{cfg.short}"
    long_col = f"SMA{cfg.long}"

    plot_df = df.copy()
    if cfg.plot_days is not None and not plot_df.empty:
        cutoff = plot_df[date_col].max() - pd.Timedelta(days=int(cfg.plot_days))
        plot_df = plot_df[plot_df[date_col] >= cutoff].copy()

    fig, ax = plt.subplots(figsize=cfg.figsize)

    if show_price:
        ax.plot(plot_df[date_col], plot_df[close_col], label="Close")

    if show_mas:
        ax.plot(plot_df[date_col], plot_df[short_col], label=f"SMA {cfg.short}")
        ax.plot(plot_df[date_col], plot_df[long_col], label=f"SMA {cfg.long}")

    if not report.events.empty:
        ev = report.events.copy()
        ev_in = ev[(ev["Date"] >= plot_df[date_col].min()) & (ev["Date"] <= plot_df[date_col].max())]

        if mark_ma_crosses:
            golden = ev_in[ev_in["type"] == "GOLDEN_CROSS"]
            death = ev_in[ev_in["type"] == "DEATH_CROSS"]
            ax.scatter(golden["Date"], golden["Close"], marker="^", s=80, label="Golden cross")
            ax.scatter(death["Date"], death["Close"], marker="v", s=80, label="Death cross")

        if mark_price_crosses and cfg.price_cross_mas:
            for period in cfg.price_cross_mas:
                up = ev_in[ev_in["type"] == f"PRICE_CROSS_UP_SMA{period}"]
                dn = ev_in[ev_in["type"] == f"PRICE_CROSS_DN_SMA{period}"]
                ax.scatter(up["Date"], up["Close"], marker="o", s=50, label=f"Price cross up SMA{period}")
                ax.scatter(dn["Date"], dn["Close"], marker="x", s=60, label=f"Price cross dn SMA{period}")

    title = cfg.title or f"{report.symbol.upper()} ({report.interval}) Close + SMA{cfg.short}/SMA{cfg.long}"
    if report.latest.get("regime"):
        title += f" — Regime: {report.latest['regime']}"

    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.show()
