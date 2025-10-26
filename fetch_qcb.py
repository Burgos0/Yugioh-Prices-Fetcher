#!/usr/bin/env python3
import datetime as dt, json, time
from pathlib import Path
import requests

CATEGORY_ID = "2"       # Yu-Gi-Oh!
GROUP_ID    = "23656"   # Quarter Century Bonanza
BASE = "https://tcgcsv.com/tcgplayer"

def fetch_json(url, retries=4, backoff=1.6, timeout=30):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code >= 500:
                raise RuntimeError(f"Server {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(backoff**i)
    raise last

def to_csv_line(pid, name, sub, rarity, printing, market, low, high, updated):
    def esc(s):
        s = "" if s is None else str(s)
        return '"' + s.replace('"','""') + '"' if (',' in s or '"' in s) else s
    return ",".join([
        pid, esc(name), esc(sub), esc(rarity), esc(printing),
        str(market if market is not None else ""),
        str(low if low is not None else ""),
        str(high if high is not None else ""),
        esc(updated or "")
    ])

def main():
    today = dt.date.today().isoformat()
    outdir = Path("data/quarter_century_bonanza")
    outdir.mkdir(parents=True, exist_ok=True)

    products_url = f"{BASE}/{CATEGORY_ID}/{GROUP_ID}/products"
    prices_url   = f"{BASE}/{CATEGORY_ID}/{GROUP_ID}/prices"

    products = fetch_json(products_url).get("results", [])
    prices   = fetch_json(prices_url).get("results", [])

    # write raw JSON snapshots
    (outdir / f"{today}__products.json").write_text(
        json.dumps({"results": products}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (outdir / f"{today}__prices.json").write_text(
        json.dumps({"results": prices}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # normalized CSV
    lookup = {str(p["productId"]): p for p in products}
    header = "productId,name,subType,rarity,printing,marketPrice,lowPrice,highPrice,lastUpdated"
    lines = [header]
    for rec in prices:
        pid = str(rec.get("productId",""))
        prod = lookup.get(pid, {})
        name = prod.get("name","")
        sub  = rec.get("subTypeName","")
        rarity = prod.get("rarity","")
        printing = rec.get("printing","")
        market = rec.get("marketPrice", None)
        low    = rec.get("lowPrice", None)
        high   = rec.get("highPrice", None)
        updated= rec.get("updatedAt") or rec.get("dateUpdated")
        lines.append(to_csv_line(pid, name, sub, rarity, printing, market, low, high, updated))

    csv_path = outdir / f"{today}.csv"
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {csv_path}")

if __name__ == "__main__":
    main()
