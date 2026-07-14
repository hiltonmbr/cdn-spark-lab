# 🐍 PySpark na Prática

## O Que é PySpark?

PySpark é a API oficial do Apache Spark para Python. Ele usa **Py4J** para fazer a ponte entre o interpretador Python e a **JVM** onde o Spark realmente executa — quando você chama `df.filter(...)` em Python, o Py4J traduz a chamada para a JVM, que a executa usando Catalyst + Tungsten. Seu código Python **não** executa distribuído entre os Executors para operações DataFrame — apenas a lógica JVM executa distribuída. O Python no Driver atua como um "controle remoto" para o motor Spark.

## PySpark vs. Pandas: Quando Usar Cada Um

| Característica | Pandas | PySpark |
|---|---|---|
| Volume de dados | Cabe na RAM de uma máquina | Distribuído entre centenas de máquinas |
| Limite prático | ~10 GB (depende da RAM) | Petabytes |
| Modelo de execução | Eager | **Lazy** (otimiza antes de executar) |
| Sobrecarga | Nenhuma (local) | Significativa (rede, serialização, JVM) |
| Casos de uso | Análise exploratória, conjuntos pequenos | ETL de Data Lake, pipelines de produção |

**Regra prática**: se seus dados cabem confortavelmente na RAM do seu laptop (até ~10 GB), use Pandas/Polars/DuckDB — mais simples e rápido. Se não cabem em uma máquina, PySpark é a escolha natural.

## SparkSession: O Ponto de Entrada

Desde o Spark 2.0, a `SparkSession` unificou os pontos de entrada antigos (`SparkContext`, `SQLContext`, `HiveContext`):

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstJob") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/bronze/vendas/")
df.show(5)
```

## Spark Connect: Um Cliente Leve para o Cluster

Tradicionalmente, a JVM do Driver precisava executar onde quer que você submetesse o job, com um caminho de rede direto para cada Executor. O **Spark Connect** (estável desde o Spark 3.5) desacopla isso: seu processo Python se torna um **cliente gRPC leve** que envia planos lógicos não resolvidos para um servidor Spark Connect remoto, que executa o Driver real e se comunica com o cluster. Seu laptop nunca precisa de uma JVM, um classpath do Hadoop ou alcance de rede direto aos Executors — apenas uma conexão gRPC.

```python
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

Isso é exatamente o que os Casos B e D deste laboratório usam: `make up-cluster` inicia um servidor `spark-connect` dentro do Docker, e seu notebook no host se conecta a ele como um cliente leve.

## DataFrames vs. RDDs, Lado a Lado

```python
# Maneira moderna: DataFrames (recomendado)
df = spark.read.csv("vendas.csv", header=True, inferSchema=True)
df.filter(df.valor > 100).groupBy("regiao").sum("valor").show()

# Maneira legada: RDDs (evite em código novo)
rdd = spark.sparkContext.textFile("vendas.csv")
rdd.filter(lambda line: "ERROR" in line).count()
```

**Recomendação atual**: prefira sempre a API DataFrame. Ela se beneficia do Catalyst e do Tungsten — otimizações que RDDs puros nunca recebem.

## O Precipício de Performance do Python: UDFs

Funções nativas (`pyspark.sql.functions.upper()`, `when()`, etc.) executam inteiramente na JVM — o Catalyst as entende e otimiza. Uma **UDF Python** quebra isso:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def normalize(name):          # executa linha por linha, em Python!
    return name.strip().upper()
```

Cada linha é **serializada (pickle) → enviada a um processo Python → executada → desserializada de volta**. O Catalyst não consegue enxergar dentro da função — sem predicate/projection pushdown, sem code generation para esse passo.

## Pandas UDFs: Vetorização via Apache Arrow

Desde o Spark 2.3, as **Pandas UDFs** usam **Apache Arrow** para trocar dados entre a JVM e o Python em **lotes colunares** (não linha por linha), aproveitando a vetorização NumPy/Pandas — tipicamente **3-100x mais rápido** que uma UDF Python comum:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def normalize(names: pd.Series) -> pd.Series:
    return names.str.strip().str.upper()
```

**Regra prática**: prefira sempre as funções nativas `pyspark.sql.functions`. Se precisar de lógica customizada, recorra a uma Pandas UDF antes de uma UDF Python comum.

## Spark SQL: Mesmo Motor, Sintaxe SQL

```python
df.createOrReplaceTempView("vendas")

result = spark.sql("""
    SELECT regiao, SUM(valor) AS total
    FROM vendas
    WHERE ano = 2026
    GROUP BY regiao
    ORDER BY total DESC
""")
```

A API DataFrame e o Spark SQL produzem o **mesmo** plano de execução física, otimizado pelo mesmo Catalyst Optimizer — a escolha entre eles é legibilidade, não performance.

## O Que Você Verá Neste Laboratório

O Lab 05 apresenta o Spark Connect na prática: você iniciará o cluster, conectará um cliente leve do seu host e confirmará (via interface do Spark em `localhost:4040`) que a execução realmente está acontecendo nos contêineres Docker, não no seu laptop.
