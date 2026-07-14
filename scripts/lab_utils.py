"""Shared utilities for all cdn-spark-lab notebooks.

Centralizes SparkSession creation for the 4 cases (local / Spark Connect /
YARN+HDFS / Spark Connect+S3), plus small helpers for the Bronze/Silver/Gold
medallion layout and the Lab 13 benchmark.

Pedagogical note: building the SparkSession *is* the lesson in Labs 01
(local), 05 (Spark Connect), and 08 (YARN) — those notebooks inline the same
`.builder...getOrCreate()` chain defined below by hand instead of calling
these factories, so the connection mechanics stay visible instead of being
hidden behind a one-line helper call. Every other notebook already saw that
reveal once and just calls the factory to keep reruns short. If you change a
factory's config here, mirror the change in the matching notebook's inlined
cell too.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ---------------------------------------------------------------------------
# SparkSession factories — one per case
# ---------------------------------------------------------------------------


def get_local_session(app_name: str = "cdn-spark-lab-local") -> SparkSession:
    """Case A: pure local mode. No Docker involved — Spark ships inside pip."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def get_connect_session(app_name: str = "cdn-spark-lab-connect") -> SparkSession:
    """Cases B and D: thin client over Spark Connect (`make up-cluster` / `make up-s3`).

    All heavy compute happens in the Docker cluster — this process only sends
    the unresolved logical plan over gRPC and receives results back.
    Remember: any file path you reference (e.g. "/data/bronze/vendas") is
    resolved *inside* the containers, not on your laptop — that's why
    docker-compose.yml mounts ./data at /data in every Spark container.
    """
    return SparkSession.builder.appName(app_name).remote("sc://localhost:15002").getOrCreate()


def get_yarn_session(app_name: str = "cdn-spark-lab-yarn") -> SparkSession:
    """Case C: client mode against a dockerized YARN cluster (`make up-hadoop`).

    The driver runs on your HOST (this process), but executors run inside
    the nodemanager1/nodemanager2 containers. For executors to call back to
    this driver, docker-compose.yml gives them `host.docker.internal` via
    Docker's `host-gateway` extra_hosts entry — hence `spark.driver.host`
    below. See docs/07-spark-and-hdfs.md for the full explanation, including why
    this case uses `webhdfs://` instead of `hdfs://` for all I/O.
    """
    import os

    # Spark needs HADOOP_CONF_DIR to find the ResourceManager. This is a
    # HOST-only config (localhost + published ports), separate from
    # config/hadoop/ (used by the containers themselves) — see
    # config/hadoop-client/ for why.
    os.environ["HADOOP_CONF_DIR"] = str(Path(__file__).resolve().parent.parent / "config" / "hadoop-client")
    # Without Kerberos, HDFS trusts whatever username the client claims.
    # Your host OS username almost certainly isn't "root" (the owner of "/"
    # in this cluster, since every container in the "hadoop" profile runs as
    # root) — this makes the driver identify as "root" too, avoiding a
    # spurious AccessControlException on YARN's staging directory.
    os.environ["HADOOP_USER_NAME"] = "root"

    return (
        SparkSession.builder.appName(app_name)
        .master("yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.host", "host.docker.internal")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.yarn.am.memory", "1g")
        # Use Spark's jars already bind-mounted into the NodeManager
        # containers (docker-compose.yml) instead of having YARN stage them
        # through HDFS — sidesteps the driver-vs-container hostname mismatch
        # for the URI that staging would otherwise bake in. See docs/07.
        .config("spark.yarn.jars", "local:/opt/spark-jars/*")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bronze / Silver / Gold path helpers — same medallion layout, different root
# ---------------------------------------------------------------------------

LAYER_ROOTS = {
    # Case A: plain host filesystem. Absolute (derived from this file's own
    # location, not the caller's cwd) so it works the same from notebooks/
    # (Jupyter's cwd), pytest (repo root), or anywhere else.
    "local": str(DATA_DIR),
    "connect": "/data",  # Cases B/D: shared volume, resolved inside containers
    "hdfs": "webhdfs://localhost:14000/datalake",  # Case C: via the HttpFS gateway
    "s3": "s3a://{layer}",  # Case D storage variant: bronze/silver/gold buckets
}


def layer_path(tier: str, layer: str, table: str) -> str:
    """Build the path for `table` in a given medallion `layer` (bronze/silver/gold)
    for the given `tier` ("local", "connect", "hdfs", or "s3")."""
    if tier == "s3":
        return f"s3a://{layer}/{table}"
    if tier == "hdfs":
        return f"{LAYER_ROOTS['hdfs']}/{layer}/{table}"
    root = LAYER_ROOTS[tier]
    return f"{root}/{layer}/{table}"


def upload_bronze_table_to_hdfs(
    table: str, webhdfs_url: str = "http://localhost:14000"
) -> None:
    """Uploads a locally generated bronze table (see generate_dataset.py) into
    HDFS through the HttpFS gateway, preserving its partition folder layout.

    Case C's executors run inside nodemanager1/nodemanager2, which have no
    access to the host's ./data — unlike Cases B/D, where the shared Docker
    volume makes the data visible to containers automatically. This is the
    one-time bootstrap step that gets it into HDFS.
    """
    from hdfs import InsecureClient

    local_root = DATA_DIR / "bronze" / table
    if not local_root.exists():
        raise FileNotFoundError(
            f"{local_root} not found — run `make generate-data` first."
        )

    client = InsecureClient(webhdfs_url, user="root")
    n = 0
    for local_file in sorted(local_root.rglob("*.parquet")):
        rel = local_file.relative_to(local_root)
        hdfs_path = f"/datalake/bronze/{table}/{rel.as_posix()}"
        with open(local_file, "rb") as f:
            client.write(hdfs_path, f, overwrite=True)
        n += 1
    print(f"Uploaded {n} file(s) to hdfs:///datalake/bronze/{table}")


# ---------------------------------------------------------------------------
# Benchmark helpers (Lab 13 — the Grand Benchmark)
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    label: str
    seconds: float
    row_count: int
    extra: dict = field(default_factory=dict)


@contextmanager
def timed():
    """Usage: with timed() as t: ... ; print(t.seconds)"""

    class _Timer:
        seconds: float = 0.0

    t = _Timer()
    start = time.perf_counter()
    yield t
    t.seconds = time.perf_counter() - start


def run_gold_benchmark(spark: SparkSession, label: str, tier: str) -> BenchmarkResult:
    """Runs the canonical Gold aggregation (vendas por setor/periodo, broadcast
    join on empresas) and times it end-to-end, including the action.
    Identical logic across all 4 tiers — only the input paths change."""
    from pyspark.sql.functions import broadcast, col
    from pyspark.sql.functions import sum as spark_sum

    vendas = spark.read.parquet(layer_path(tier, "bronze", "vendas"))
    empresas = spark.read.parquet(layer_path(tier, "bronze", "empresas"))

    with timed() as t:
        gold = (
            vendas.join(broadcast(empresas), "id_empresa")
            .groupBy("setor", "ano", "mes")
            .agg(spark_sum("valor").alias("total_vendas"))
            .orderBy(col("total_vendas").desc())
        )
        row_count = gold.count()

    return BenchmarkResult(label=label, seconds=t.seconds, row_count=row_count)
