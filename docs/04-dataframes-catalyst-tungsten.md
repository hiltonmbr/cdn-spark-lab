# 📊 DataFrames, Spark SQL, Catalyst & Tungsten

## Why RDDs Weren't Enough

RDDs are powerful, but Spark doesn't understand the internal structure of the data they carry — to the engine, an RDD is just an opaque collection of Python/Java/Scala objects. If you have an RDD of `(name, age, city)` records and want to sum just the `age` column, Spark must deserialize the **entire** object per record, even if it only needs one field. There's no way to apply classic relational optimizations, because the engine doesn't know "this is a table with columns."

The fix: give Spark a **structured, typed** view of the data — DataFrames and Spark SQL (2015).

## DataFrame: RDDs with a Schema

A DataFrame is conceptually equivalent to a relational database table or a Pandas DataFrame: data organized into named, typed rows and columns. Because Spark **knows** the schema, it can apply optimizations impossible on a generic RDD — like reading only the needed columns from a Parquet file, or reordering filters to discard data as early as possible.

## Catalyst Optimizer

Catalyst is Spark SQL's query optimizer. It turns your code into a highly efficient execution plan through four phases:

1. **Analysis** — resolves column names and types against the catalog.
2. **Logical Optimization** — applies rules like *predicate pushdown* (push filters as early as possible) and *constant folding*.
3. **Physical Planning** — generates multiple candidate physical plans and picks the cheapest via a cost model.
4. **Code Generation** — compiles parts of the physical plan directly into JVM bytecode (*whole-stage code generation*), eliminating interpretation overhead.

### Predicate Pushdown in Action

Query: `SELECT nome FROM clientes WHERE regiao = 'Nordeste'`, reading from a Parquet file partitioned by `regiao`.

- **Without optimization**: Spark reads **every** Parquet file, decodes **every** column of **every** record, then applies the filter and discards unused columns.
- **With Catalyst** (predicate + projection pushdown): Spark identifies it only needs the `nome` column, and that the filter can be applied **during read** — skipping entire Parquet row-groups/partitions that don't match, never decoding columns that won't be used.

This is exactly why columnar formats like **Parquet** became the Data Lake standard — they let optimizers like Catalyst skip irrelevant data without even reading it from disk.

## Tungsten: Memory & CPU-Level Optimization

While Catalyst optimizes **what** to execute, **Project Tungsten** optimizes **how** to execute at the hardware level:

- **Off-heap memory management** — represents data in a compact binary format, avoiding JVM object overhead (headers, garbage collector) and drastically reducing memory use.
- **Cache-aware computation** — organizes algorithms and data structures to maximize CPU L1/L2/L3 cache hits.
- **Whole-stage code generation** — compiles entire operator chains into a single optimized bytecode loop, as if an engineer hand-wrote specialized Java code for that exact query.

## Reading the Execution Plan

```python
df.explain(True)   # logical + physical plan, right in the terminal
```

In the Spark UI's **SQL** tab, look for:

- **BroadcastHashJoin** vs. **SortMergeJoin** — if a small table is doing a SortMerge, force `broadcast()` to eliminate the Shuffle.
- **Filter** placed **before** a **Scan** — indicates predicate pushdown is working.
- **Exchange** (= Shuffle) — every Exchange node in the plan is a network redistribution point; fewer is better.
- **WholeStageCodegen** — indicates Tungsten is compiling operations into optimized bytecode.

## What You'll See in This Lab

Lab 04 (Case A) runs `.explain(True)` on the same query as raw RDD operations vs. DataFrame API, side by side, so you can see the plan Catalyst produces that the RDD path never gets.
