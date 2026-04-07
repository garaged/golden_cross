from golden_cross import MAConfig, ma_cross_report


cfg = MAConfig(short=50, long=200, price_cross_mas=(50,))

spx = ma_cross_report("^spx", cfg=cfg, plot=False)
spy = ma_cross_report("spy.us", cfg=cfg, plot=False)
orcl = ma_cross_report("orcl.us", cfg=cfg, plot=False)
btc = ma_cross_report("btcusd", cfg=cfg, plot=False)

btc.events["diff"] = btc.events["Date"].diff().dt.days
print(btc.events.tail())
