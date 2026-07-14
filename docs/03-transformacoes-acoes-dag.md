# 🔀 Transformações, Ações e o DAG

## Duas Categorias de Operações

Toda operação em um RDD (ou DataFrame) se enquadra em uma de duas categorias:

- **Transformações** — recebem um RDD, produzem outro RDD. **Lazy**: não executam imediatamente, apenas são registradas na linhagem. Exemplos: `map()`, `filter()`, `flatMap()`, `groupByKey()`, `reduceByKey()`, `join()`, `union()`.
- **Ações** — disparam a **execução real** de toda a cadeia de transformações acumulada, retornando um resultado para o Driver ou gravando em armazenamento. Exemplos: `count()`, `collect()`, `first()`, `take(n)`, `reduce()`, `saveAsTextFile()`.

## Por Que Avaliação Lazy?

Diferente do MapReduce clássico (que executa cada fase imediatamente), o Spark **acumula** transformações sem executá-las até que uma ação seja chamada. Isso desbloqueia três ganhos reais de performance:

- **Visão global do pipeline** — o Catalyst Optimizer (para DataFrames) enxerga a **cadeia inteira** de operações antes de executar qualquer coisa, podendo reordenar, mesclar ou eliminar passos desnecessários.
- **Evita trabalho desperdiçado** — se você encadeia 5 transformações mas só pede `take(10)`, Spark processa apenas o suficiente para entregar essas 10 linhas, não o conjunto inteiro.
- **Menos passagens nos dados** — `filter()` seguido de `map()` pode ser **fusionado** em uma única passagem por registro em vez de duas.

```python
logs = spark.sparkContext.textFile("access.log")

# Nada executa ainda — estas são apenas Transformações
errors = logs.filter(lambda line: "ERROR" in line)
words = errors.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda w: (w, 1))

# SÓ AGORA o Spark constrói o DAG e executa tudo de uma vez:
result = pairs.reduceByKey(lambda a, b: a + b).collect()
```

Até a linha do `collect()`, nenhum byte do arquivo de log foi lido.

## O DAG: Directed Acyclic Graph

Toda vez que uma ação é chamada, o Driver converte a cadeia de transformações acumulada em um **DAG** — um grafo onde cada nó é um RDD e cada aresta é uma transformação. O DAG Scheduler analisa este grafo e o divide em **Stages** — grupos de transformações que podem executar sem mover dados entre máquinas (sem *shuffle*).

"Acíclico" é literal: o grafo nunca faz loops. Cada RDD depende apenas de RDDs "anteriores" no pipeline, então Spark sempre sabe a ordem correta de execução e pode paralelizar tudo sem dependências mútuas.

## Dependências Narrow vs. Wide

- **Narrow** — cada partição do RDD "filho" depende de **no máximo uma** partição do RDD "pai" (ex.: `map()`, `filter()`). Executa **dentro do mesmo Executor**, sem transferência de rede — extremamente rápida.
- **Wide** — uma partição do RDD "filho" depende de **várias** partições do RDD "pai", espalhadas por diferentes Executors (ex.: `groupByKey()`, `reduceByKey()`, `join()`). Requer um **Shuffle** — dados cruzam a rede entre Executors.

## Jobs, Stages, Tasks

1. **Job** — criado a cada chamada de **ação**. Uma aplicação pode disparar dezenas de Jobs.
2. **Stage** — cada Job divide-se em Stages, delimitadas por pontos de **Shuffle** (dependências wide). Tudo dentro de uma Stage é dependência narrow e se encadeia sem movimento de rede.
3. **Task** — cada Stage divide-se em Tasks — uma Task por partição. A menor unidade de trabalho, enviada para um único core em um único Executor.

## O Shuffle: O Gargalo do Spark

Assim como o Shuffle & Sort era o calcanhar de Aquiles do MapReduce, o **Shuffle** continua sendo a operação mais cara do Spark — mesmo com todo o resto em memória. Um shuffle requer que **cada** Executor grave dados intermediários em disco local (arquivos de shuffle), então **outros** Executors os leiam pela rede, redistribuindo registros por chave (um padrão todos-para-todos). Envolve serialização/desserialização, E/S de disco e tráfego de rede entre cada par de Executors.

## A Interface Web do Spark

Toda aplicação Spark expõe uma interface web (porta **4040** por padrão) com visibilidade completa da execução: Jobs, Stages (incluindo se houve Shuffle), Tasks (temporização individual — essencial para detectar **data skew**), Storage (RDDs/DataFrames em cache) e SQL (plano de execução física do Catalyst). Diagnosticar lentidão no Spark quase sempre começa aqui.

## O Que Você Verá Neste Laboratório

O Lab 06 (Caso B) deliberadamente dispara um join pesado com Shuffle (`funcionarios` — sem broadcast) lado a lado com um join sem Shuffle via broadcast (`empresas`), e faz você comparar ambos na aba Stages da interface do Spark: conte os nós Exchange, compare as durações das Stages e perceba a diferença que um Shuffle faz.
