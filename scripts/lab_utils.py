"""Utilitários compartilhados para todos os notebooks do cdn-spark-lab.

Centraliza a criação da SparkSession para os 4 casos (local / Spark Connect /
YARN+HDFS / Spark Connect+S3), além de pequenos auxiliares para o layout
medallion Bronze/Silver/Gold e o benchmark do Lab 13.

Nota pedagógica: construir a SparkSession *é* a lição nos Labs 01
(local), 05 (Spark Connect) e 08 (YARN) — esses notebooks inserem a mesma
cadeia `.builder...getOrCreate()` definida abaixo manualmente em vez de chamar
essas fábricas, para que a mecânica da conexão permaneça visível em vez de ficar
oculta atrás de uma chamada de uma linha. Todos os outros notebooks já viram essa
revelação uma vez e apenas chamam a fábrica para manter as reexecuções curtas. Se você
alterar a configuração de uma fábrica aqui, espelhe a alteração na célula correspondente
do notebook também.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# ---------------------------------------------------------------------------
# Fábricas de SparkSession — uma por caso
# ---------------------------------------------------------------------------


def get_local_session(app_name: str = "cdn-spark-lab-local") -> SparkSession:
    """Caso A: modo local puro. Sem Docker — o Spark já vem instalado via pip."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def get_connect_session(app_name: str = "cdn-spark-lab-connect") -> SparkSession:
    """Casos B e D: cliente leve sobre Spark Connect (`make up-cluster` / `make up-s3`).

    Todo o processamento pesado ocorre no cluster Docker — este processo apenas envia
    o plano lógico não resolvido via gRPC e recebe os resultados de volta.
    Lembre-se: qualquer caminho de arquivo que você referenciar (ex.: "/data/bronze/vendas") é
    resolvido *dentro* dos contêineres, não no seu laptop — é por isso que
    o docker-compose.yml monta ./data em /data em cada contêiner Spark.
    """
    return SparkSession.builder.appName(app_name).remote("sc://localhost:15002").getOrCreate()


def get_yarn_session(app_name: str = "cdn-spark-lab-yarn") -> SparkSession:
    """Caso C: modo client contra um cluster YARN dockerizado (`make up-hadoop`).

    O driver executa no seu HOST (este processo), mas os executores rodam dentro
    dos contêineres nodemanager1/nodemanager2. Para que os executores chamem de volta
    este driver, o docker-compose.yml fornece `host.docker.internal` via
    entrada extra_hosts `host-gateway` do Docker — daí o `spark.driver.host`
    abaixo. Consulte docs/07-spark-e-hdfs.md para a explicação completa, incluindo por que
    este caso usa `webhdfs://` em vez de `hdfs://` para toda E/S.
    """
    import os

    # O Spark precisa do HADOOP_CONF_DIR para encontrar o ResourceManager. Esta é uma
    # configuração APENAS do HOST (localhost + portas publicadas), separada do
    # config/hadoop/ (usado pelos próprios contêineres) — consulte
    # config/hadoop-client/ para entender o motivo.
    os.environ["HADOOP_CONF_DIR"] = str(Path(__file__).resolve().parent.parent / "config" / "hadoop-client")
    # Sem Kerberos, o HDFS confia em qualquer nome de usuário que o cliente informar.
    # O nome de usuário do seu sistema host quase certamente não é "root" (o proprietário de "/"
    # neste cluster, já que todo contêiner no perfil "hadoop" executa como
    # root) — isso faz o driver se identificar como "root" também, evitando um
    # AccessControlException espúrio no diretório de staging do YARN.
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
        # Usa os jars do Spark já montados nos contêineres NodeManager
        # (docker-compose.yml) em vez de fazer o YARN distribuí-los
        # via HDFS — contorna a incompatibilidade de hostname entre driver e contêiner
        # que o staging incorporaria. Consulte docs/07.
        .config("spark.yarn.jars", "local:/opt/spark-jars/*")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Auxiliares de caminho Bronze / Silver / Gold — mesmo layout medallion, raiz diferente
# ---------------------------------------------------------------------------

LAYER_ROOTS = {
    # Caso A: sistema de arquivos local absoluto. Absoluto (derivado da localização deste
    # arquivo, não do cwd do chamador) para funcionar igualmente a partir de notebooks/
    # (cwd do Jupyter), pytest (raiz do repositório) ou qualquer outro lugar.
    "local": str(DATA_DIR),
    "connect": "/data",  # Casos B/D: volume compartilhado, resolvido dentro dos contêineres
    "hdfs": "webhdfs://localhost:14000/datalake",  # Caso C: via gateway HttpFS
    "s3": "s3a://{layer}",  # Variante de armazenamento Caso D: buckets bronze/silver/gold
}


def layer_path(tier: str, layer: str, table: str) -> str:
    """Constrói o caminho para `table` em uma determinada camada medallion `layer` (bronze/silver/gold)
    para o `tier` informado ("local", "connect", "hdfs" ou "s3")."""
    if tier == "s3":
        return f"s3a://{layer}/{table}"
    if tier == "hdfs":
        return f"{LAYER_ROOTS['hdfs']}/{layer}/{table}"
    root = LAYER_ROOTS[tier]
    return f"{root}/{layer}/{table}"


def upload_bronze_table_to_hdfs(
    table: str, webhdfs_url: str = "http://localhost:14000"
) -> None:
    """Faz upload de uma tabela bronze gerada localmente (consulte generate_dataset.py) para
    o HDFS através do gateway HttpFS, preservando a estrutura de pastas particionadas.

    Os executores do Caso C rodam dentro de nodemanager1/nodemanager2, que não têm
    acesso ao ./data do host — diferente dos Casos B/D, onde o volume compartilhado
    do Docker torna os dados visíveis para os contêineres automaticamente. Este é o
    passo de inicialização único que coloca os dados no HDFS.
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
# Auxiliares de benchmark (Lab 13 — O Grande Benchmark)
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    label: str
    seconds: float
    row_count: int
    extra: dict = field(default_factory=dict)


@contextmanager
def timed():
    """Uso: with timed() as t: ... ; print(t.seconds)"""

    class _Timer:
        seconds: float = 0.0

    t = _Timer()
    start = time.perf_counter()
    yield t
    t.seconds = time.perf_counter() - start


def run_gold_benchmark(spark: SparkSession, label: str, tier: str) -> BenchmarkResult:
    """Executa a agregação Gold canônica (vendas por setor/período, broadcast
    join em empresas) e mede o tempo total, incluindo a ação.
    Lógica idêntica em todos os 4 tiers — apenas os caminhos de entrada mudam."""
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
