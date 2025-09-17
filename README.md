Market Structure Analyzer — PoC

What this proves:

Connects to a real exchange WebSocket (Binance Spot aggTrade).

Normalizes messages to rows and writes Parquet files partitioned by date.

Optionally uploads to Amazon S3 (if AWS credentials are configured).
Docs: Binance WebSocket streams and update/connection rules. 

Environment

Ubuntu or Windows

Python: 3.11.8 (CPython)

Compiler: N/A (Python project). Uses pip to install C wheels for pyarrow.