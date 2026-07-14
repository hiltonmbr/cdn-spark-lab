# ☁️ Spark + Object Storage (S3 / RustFS)

## Spark with S3-Compatible Storage

As covered in the Object Storage module, the modern trend is S3-compatible storage (AWS S3, RustFS, MinIO) instead of HDFS. Spark connects to any of them via the `s3a://` protocol — the Hadoop-AWS connector:

```python
df = spark.read.parquet("s3a://silver/vendas/")
df.write.mode("overwrite").parquet("s3a://gold/relatorio/")
```

**The same code works against AWS S3, RustFS, and MinIO** — only the endpoint configuration changes. That's the real advantage of S3 as an open protocol: Spark doesn't need to know whether it's talking to AWS or an on-premises server.

## Configuring the S3 Connector

```python
spark = SparkSession.builder \
    .appName("SparkWithS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs-server:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "adminpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

**`path.style.access = true` is mandatory** for on-premises servers (RustFS, MinIO). AWS S3 defaults to *virtual-hosted style*, while on-prem solutions use *path style* — this flag is the difference between working and silently failing.

In this lab, that configuration already lives in the `spark-connect` service's startup command in `docker-compose.yml` — your notebook, connecting as a thin Spark Connect client, doesn't need to repeat it.

## S3 vs. HDFS in Spark: What Changes?

| Aspect | HDFS | Object Storage (S3) |
|---|---|---|
| URI scheme | `hdfs://namenode:8020/...` | `s3a://bucket/prefix/...` |
| Data locality | ✅ Tasks run near the data | ❌ Data comes over the network (no locality) |
| Atomic rename | ✅ Fast (NameNode metadata) | ⚠️ Slow (copy + delete) — impacts commits |
| Consistency | ✅ Immediate | ✅ Eventual → strong (S3 since 2020) |
| Scalability | Limited by the NameNode | Virtually unlimited |
| Cost | Dedicated 24/7 cluster | Pay only for what you store |

**Performance tip**: since S3 has no data locality, the bottleneck shifts to **network, not disk**. Columnar formats (Parquet) and **column partitioning** (e.g. `ano=2026/mes=07/`) matter even more here than on HDFS — they're what lets Spark skip reading data it doesn't need.

## Why Object Storage Commits Are Slower

Traditional filesystems (including HDFS) implement `rename()` as an O(1) metadata operation — flipping a pointer. Object stores don't have a native rename: renaming a "file" means **copying** every byte to the new key, then deleting the old one. Spark's default commit protocol relies on renames to atomically publish job output, so on S3 this can become a meaningful tax on large writes — one of the reasons formats like Delta Lake and Iceberg exist (they replace rename-based commits with a transaction log).

## What You'll See in This Lab

- Lab 11 connects to RustFS via `s3a://` and runs the same Bronze→Silver→Gold pipeline as Cases B and C, writing into the `bronze`/`silver`/`gold` buckets created by `make up-s3`.
- Lab 12 benchmarks the same write operation against HDFS (Case C) and RustFS (Case D), measuring commit/rename overhead directly, and demonstrates partition pruning by comparing a full-table scan against a filtered read on `ano=2026/mes=07/`.
