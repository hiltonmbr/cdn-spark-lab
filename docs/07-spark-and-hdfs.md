# 📂 Spark + HDFS (via YARN)

## Reading and Writing HDFS

Spark is not a storage system — it reads and writes external sources. HDFS integration is native:

```python
# Native RPC scheme, used when the Driver runs inside the cluster
df = spark.read.text("hdfs://namenode:8020/user/data/logs.txt")
df.write.mode("overwrite").parquet("hdfs://namenode:8020/lake/gold/report/")
```

**Careful**: the output directory must **not** already exist (unless you use `.mode("overwrite")`). Spark writes partitioned output — the "output file" is actually a **directory** containing multiple `part-00000`, `part-00001`, etc., one per partition.

## File Formats: Which One to Pick?

| Format | Type | Compression | Partial reads | When to use |
|---|---|---|---|---|
| **Parquet** | Columnar | Snappy/GZIP | ✅ Columns + partitions | **Data Lake default** — best fit for Catalyst pushdown |
| **ORC** | Columnar | ZLIB/Snappy | ✅ Columns + partitions | Parquet alternative, popular in the Hive ecosystem |
| **Avro** | Row | Deflate/Snappy | ❌ Reads everything | Ideal for **streaming** and frequently evolving schemas |
| **CSV/JSON** | Text | Optional | ❌ Reads everything | Interoperability, but **inefficient** for Big Data |
| **Delta Lake** | Columnar+ACID | Snappy | ✅ Columns + partitions | Parquet with ACID transactions and time travel |

**Rule of thumb**: for Big Data pipelines, **Parquet is the de facto default** — the same format used in the Hadoop and Object Storage modules' Silver/Gold layers. It lets Catalyst do predicate and projection pushdown, reading only the columns and partitions actually needed.

## Why This Lab Uses `webhdfs://`, Not `hdfs://`

In a textbook Spark-on-YARN deployment, the Driver runs **inside** the cluster (`--deploy-mode cluster`), on the same network as the NameNode and DataNodes — so `hdfs://namenode:8020/...` resolves cleanly for everyone involved.

This lab intentionally keeps the Driver on **your host** for every case (Case C included), so `make jupyter-lab` is the same command everywhere. That has a real consequence: your Driver process needs to resolve the NameNode's hostname and, for native RPC, every DataNode's hostname too (block transfers are direct DataNode↔client). Docker containers aren't addressable by name from the host without extra host-level configuration.

The fix already running in `docker-compose.yml`'s `hadoop` profile: an **HttpFS gateway** (`proxy` service, port `14000`) that speaks the WebHDFS REST protocol and internally proxies *all* NameNode/DataNode communication — including the data transfer, not just metadata. Your Driver only ever needs to know **one** hostname (`localhost:14000`), regardless of how many DataNodes exist behind it:

```python
df = spark.read.parquet("webhdfs://localhost:14000/datalake/bronze/vendas")
df.write.mode("overwrite").parquet("webhdfs://localhost:14000/datalake/silver/vendas")
```

Executors (running inside `nodemanager1`/`nodemanager2` containers, on the same Docker network as the gateway) use the exact same URL — Spark passes the path string as-is to every worker, so there's no dual-scheme trick needed.

> 💡 **This is itself a teaching moment.** The friction you're avoiding here — hostname resolution across the client/cluster boundary — is precisely why real production Spark-on-YARN jobs run in `--deploy-mode cluster`, not `client`, as covered in docs/01. You're seeing, hands-on, why that recommendation exists.

## Configuring HDFS Access

In a "textbook" (Driver-inside-cluster) deployment, Spark needs Hadoop's config files on its classpath:

- **`core-site.xml`** — the NameNode's address (e.g. `hdfs://namenode:8020`).
- **`hdfs-site.xml`** — replication factor, block size, storage paths.

**In practice**: when Spark runs on **YARN**, these are inherited automatically from the Hadoop installation. In **cloud** environments (EMR, Dataproc), the provider already configures S3/GCS access. Manual configuration is only needed for **Standalone** or **Kubernetes** clusters connecting to an external HDFS — which is exactly this lab's `webhdfs://` situation, worked around at the gateway level instead of the classpath level.

## What You'll See in This Lab

- Lab 08 writes the Bronze→Silver→Gold pipeline to HDFS via the gateway and compares against Case B's shared-volume version.
- Lab 09 walks through `spark-submit`, deploy modes, and the YARN ResourceManager UI (`localhost:8088`) — watch the Application Master and executor containers get scheduled live.
- Lab 10 (🔥 chaos lab) kills a NodeManager mid-job and watches YARN reschedule the lost Tasks on the surviving one, recomputing only what was lost via lineage.
