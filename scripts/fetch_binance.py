#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binanceの1分足を取得し、M5/H1/D1に集計して
docs/prices/spot/YYYY-MM-DD.json に出力します（キー不要）。
"""

import argparse, json
from urllib.request import urlopen
from datetime import datetime, timezone, date

BINANCE = "https://api.binance.com/api/v3/klines"

def get_klines(symbol="BTCUSDT", interval="1m", limit=720):
    url = f"{BINANCE}?symbol={symbol}&interval={interval}&limit={limit}"
    with urlopen(url, timeout=20) as r:
        return json.loads(r.read())

def to_rows(raw):
    # [openTime, open, high, low, close, volume, closeTime, ...]
    rows=[]
    for x in raw:
        rows.append({
            "t_ms": int(x[0]),
            "o": float(x[1]),
            "h": float(x[2]),
            "l": float(x[3]),
            "c": float(x[4]),
            "v": float(x[5]),
        })
    return rows

def iso_z(ms):
    return datetime.utcfromtimestamp(ms/1000).replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")

def ohlc_agg(buf):
    if not buf: return None
    o = buf[0]["o"]
    h = max(r["h"] for r in buf)
    l = min(r["l"] for r in buf)
    c = buf[-1]["c"]
    v = sum(r["v"] for r in buf)
    t = iso_z(buf[0]["t_ms"])
    return {"t": t, "o": o, "h": h, "l": l, "c": c, "v": v}

def resample_m5(m1):
    out, buf = [], []
    for r in m1:
        minute = (r["t_ms"] // 60000) % 60
        buf.append(r)
        if minute % 5 == 4:
            out.append(ohlc_agg(buf)); buf=[]
    if buf: out.append(ohlc_agg(buf))
    return out

def resample_h1(m1):
    out, buf = [], []
    for r in m1:
        minute = (r["t_ms"] // 60000) % 60
        buf.append(r)
        if minute == 59:
            out.append(ohlc_agg(buf)); buf=[]
    if buf: out.append(ohlc_agg(buf))
    return out

def resample_d1(m1):
    out, buf, prev = [], [], None
    for r in m1:
        d = datetime.utcfromtimestamp(r["t_ms"]/1000).date()
        if prev is None: prev = d
        if d != prev:
            out.append(ohlc_agg(buf)); buf=[r]; prev = d
        else:
            buf.append(r)
    if buf: out.append(ohlc_agg(buf))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--limit", type=int, default=720)     # 12時間分(M1)くらい（必要に応じ増やす）
    ap.add_argument("--outdir", required=True)            # docs/prices/spot
    args = ap.parse_args()

    raw = get_klines(args.symbol, "1m", args.limit)
    m1  = to_rows(raw)
    m5  = resample_m5(m1)
    h1  = resample_h1(m1)
    d1  = resample_d1(m1)

    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
    base = {
        "schema_version": "1.0",
        "generated_at": now_iso,
        "series": []
    }
    def push(tf, bars):
        if not bars: return
        base["series"].append({
            "symbol": "BTCUSD",
            "class": "crypto",
            "tf": tf,                 # "M5" / "H1" / "D1"
            "base": "BTC",
            "quote": "USD",
            "bars": bars[-120:]       # 最大120本に丸め
        })
    push("M5", m5)
    push("H1", h1)
    push("D1", d1)

    ymd = date.today().isoformat()
    path = f"{args.outdir}/{ymd}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(base, f, ensure_ascii=False)
    print("written:", path)

if __name__ == "__main__":
    main()

