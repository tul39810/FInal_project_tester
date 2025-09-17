
# Minimal PoC: subscribe to Binance aggTrade stream, write Parquet locally,
# optionally upload to S3 if --s3-bucket is provided.
# Tested on: Ubuntu 22.04, Python 3.11

import asyncio, json, os, time, argparse, datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import websockets  # pip install websockets
except ImportError:
    raise SystemExit("Missing dependency: websockets")

def write_parquet(rows, out_dir):
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df["ts_iso"] = pd.to_datetime(df["ts_ms"], unit="ms")
    date = df["ts_iso"].dt.date.min().isoformat()

    out_part_dir = os.path.join(out_dir, f"dt={date}")
    os.makedirs(out_part_dir, exist_ok=True)
    out_path = os.path.join(out_part_dir, f"trades_{int(time.time())}.parquet")

    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)
    return out_path

async def collect(symbol: str, seconds: int):
    # binace docs https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
    uri = f"wss://stream.binance.com:9443/ws/{symbol}@aggTrade"
    rows = []
    deadline = time.time() + seconds

    async with websockets.connect(uri, ping_interval=20, ping_timeout=60) as ws:
        while time.time() < deadline:
            
            msg = await asyncio.wait_for(ws.recv(), timeout=30)
            d = json.loads(msg)
            rows.append({

                "exchange": "binance",
                "symbol": symbol.upper(),
                "ts_ms": int(d["E"]),           # event time (ms)
                "trade_id": int(d["a"]),
                "price": float(d["p"]),
                "qty": float(d["q"]),
                "is_buyer_maker": bool(d["m"])

            })

    return rows

def upload_s3(local_path, bucket, prefix):
    import boto3

    s3 = boto3.client("s3")

    key = f"{prefix.rstrip('/')}/{os.path.basename(os.path.dirname(local_path))}/{os.path.basename(local_path)}"
    
    s3.upload_file(local_path, bucket, key)

    return f"s3://{bucket}/{key}"

async def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--symbol", default="btcusdt", help="e.g., btcusdt, ethusdt")
    ap.add_argument("--seconds", type=int, default=60, help="collect duration")
    ap.add_argument("--out", default="data", help="local output directory")
    ap.add_argument("--s3-bucket", help="optional S3 bucket to upload parquet")
    ap.add_argument("--s3-prefix", default="msa-poc", help="S3 prefix (folder)")
    args = ap.parse_args()

    rows = await collect(args.symbol.lower(), args.seconds)
    local_path = write_parquet(rows, args.out)
    print(f"wrote parquet: {local_path}")

    if args.s3_bucket and local_path:
        s3_uri = upload_s3(local_path, args.s3_bucket, args.s3_prefix)
        print(f"uploaded to: {s3_uri}")

if __name__ == "__main__":
    asyncio.run(main())
