# 💾 Persistência e Otimização

## Cache e Persist: Reutilizando Dados

Por padrão, o Spark **recomputa** um RDD/DataFrame toda vez que uma ação é chamada sobre ele — lembre-se, a avaliação é lazy e nada é armazenado automaticamente. Quando o mesmo dado é usado **múltiplas vezes**, isso desperdiça trabalho.

```python
vendas = spark.read.parquet("data/bronze/vendas/").filter("valor > 0")
vendas.cache()  # marca para retenção em memória após o primeiro uso

total = vendas.count()                           # computa e ARMAZENA EM CACHE
avg = vendas.agg({"valor": "avg"}).collect()      # reutiliza o cache!
```

Sem `cache()`, a segunda ação (`agg`) recomputaria o **pipeline inteiro** de leitura+filtro do zero — exatamente como a primeira fez. Com cache, ela reutiliza o resultado já materializado na RAM dos Executors.

## Níveis de Armazenamento

`persist()` (mais geral que `cache()`, que é apenas um atalho para `MEMORY_ONLY`) permite escolher exatamente onde e como armazenar dados em cache:

| Nível de Armazenamento | Onde | Quando usar |
|---|---|---|
| `MEMORY_ONLY` | RAM (desserializado) | Padrão; mais rápido, mas perde dados se não couber na RAM |
| `MEMORY_AND_DISK` | RAM, transfere para disco se necessário | Mais seguro; evita recomputação total quando a RAM não é suficiente |
| `MEMORY_ONLY_SER` | RAM (serializado/compacto) | Economiza RAM ao custo de CPU extra para (de)serialização |
| `DISK_ONLY` | Apenas disco | Conjuntos grandes demais para RAM, mas ainda evita reler a fonte original |

Chame `unpersist()` quando um conjunto em cache não for mais reutilizado, para liberar memória para outras operações.

## Broadcast Variables: Evitando Shuffle em Joins

Um dos usos mais poderosos de variáveis compartilhadas é o **Broadcast Join**: quando uma tabela é pequena (cabe na memória de um único nó) e a outra é enorme, replicar a pequena para cada Executor é muito mais barato que fazer shuffle de ambas.

- **Sem broadcast (Shuffle Join)**: ambas as tabelas — mesmo a pequena — são particionadas e sofrem shuffle pela rede para que chaves correspondentes caiam no mesmo Executor. Caro, especialmente quando a tabela grande tem bilhões de linhas.
- **Com broadcast**: a tabela pequena (ex.: uma tabela `categorias` de 50 linhas) é copiada **inteira** na memória de cada Executor. O join então acontece **localmente**, com zero Shuffle da tabela grande.

```python
from pyspark.sql.functions import broadcast

categorias = spark.read.parquet("data/bronze/categorias/")  # pequena: 50 linhas
vendas = spark.read.parquet("data/bronze/vendas/")            # enorme: milhões de linhas

result = vendas.join(broadcast(categorias), "id_categoria")
```

O Spark SQL na verdade tenta detectar isso automaticamente via `spark.sql.autoBroadcastJoinThreshold` (padrão: 10 MB) — o `broadcast()` explícito é útil quando você quer **forçar** esta estratégia mesmo que a estimativa automática do otimizador esteja errada.

## Accumulators

Enquanto as broadcast variables enviam dados **Driver → Executors (somente leitura)**, os **Accumulators** vão no sentido oposto: Executors **agregam** valores de volta ao Driver com segurança, sem condições de corrida.

```python
error_count = spark.sparkContext.accumulator(0)

def process_line(line):
    if "ERROR" in line:
        error_count.add(1)
    return line.upper()

logs.map(process_line).count()  # ação que dispara o processamento
print(f"Total errors: {error_count.value}")
```

**Cuidado**: accumulators só garantem contagens exatas dentro de **ações**. Se usados dentro de uma transformação que for reexecutada (ex.: devido a uma retentativa de Task), o valor pode ser contado mais de uma vez — use-os para métricas de depuração, não para lógica crítica de negócio.

## Reparticionamento Inteligente

Operações caras (`groupBy`, `join`, `distinct`) exigem um Shuffle. Reparticionar por uma chave conhecida **uma vez**, de antemão, pode tornar o resultado reutilizável em várias operações subsequentes:

```python
vendas_by_regiao = vendas.repartition(200, "regiao")

vendas_by_regiao.groupBy("regiao").sum("valor")   # sem shuffle adicional
vendas_by_regiao.groupBy("regiao").count()          # sem shuffle adicional
```

`repartition()` custa um Shuffle, mas se o mesmo particionamento for reutilizado por **múltiplas** operações downstream, o custo total pode ser menor que pagar pelo Shuffle repetidamente. `coalesce()` reduz o número de partições **sem** um Shuffle completo — ideal após um `filter()` agressivo que deixou partições pequenas demais.

## Adaptive Query Execution (AQE)

Desde o Spark 3.0 (padrão desde o 3.2, `spark.sql.adaptive.enabled=true`), o Spark pode **replanejar a execução em tempo real** usando estatísticas coletadas durante o Shuffle, em vez de depender apenas de estimativas pré-execução:

- **Post-Shuffle partition coalescing** — reduz o número de partições de saída quando elas se mostram menores que o esperado.
- **Join strategy switching** — converte um `SortMergeJoin` em um `BroadcastHashJoin` em tempo de execução, se uma tabela se mostrar pequena o suficiente — mesmo que a estimativa original estivesse errada.
- **Skew Join Optimization** — detecta partições desproporcionalmente grandes (data skew) e automaticamente as divide em sub-partições menores processadas em paralelo.

## O Que Você Verá Neste Laboratório

O Lab 07 (Caso B) executa a mesma agregação com AQE ativado e desativado (`spark.conf.set("spark.sql.adaptive.enabled", ...)`) contra uma fatia deliberadamente distorcida de `vendas`, para que você veja o Skew Join Optimization do AQE entrar em ação ao vivo na interface do Spark.
