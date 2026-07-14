# 📊 DataFrames, Spark SQL, Catalyst e Tungsten

## Por Que RDDs Não Eram Suficientes

RDDs são poderosos, mas o Spark não entende a estrutura interna dos dados que eles carregam — para o motor, um RDD é apenas uma coleção opaca de objetos Python/Java/Scala. Se você tem um RDD de registros `(nome, idade, cidade)` e quer somar apenas a coluna `idade`, o Spark precisa desserializar o **objeto inteiro** por registro, mesmo que só precise de um campo. Não há como aplicar otimizações relacionais clássicas, porque o motor não sabe que "isto é uma tabela com colunas."

A solução: dar ao Spark uma visão **estruturada e tipada** dos dados — DataFrames e Spark SQL (2015).

## DataFrame: RDDs com um Schema

Um DataFrame é conceitualmente equivalente a uma tabela de banco de dados relacional ou a um Pandas DataFrame: dados organizados em linhas e colunas nomeadas e tipadas. Como o Spark **conhece** o schema, ele pode aplicar otimizações impossíveis em um RDD genérico — como ler apenas as colunas necessárias de um arquivo Parquet, ou reordenar filtros para descartar dados o mais cedo possível.

## Catalyst Optimizer

Catalyst é o otimizador de consultas do Spark SQL. Ele transforma seu código em um plano de execução altamente eficiente através de quatro fases:

1. **Analysis** — resolve nomes e tipos de colunas contra o catálogo.
2. **Logical Optimization** — aplica regras como *predicate pushdown* (empurrar filtros o mais cedo possível) e *constant folding*.
3. **Physical Planning** — gera múltiplos planos físicos candidatos e escolhe o mais barato via um modelo de custo.
4. **Code Generation** — compila partes do plano físico diretamente em bytecode JVM (*whole-stage code generation*), eliminando a sobrecarga de interpretação.

### Predicate Pushdown em Ação

Consulta: `SELECT nome FROM clientes WHERE regiao = 'Nordeste'`, lendo de um arquivo Parquet particionado por `regiao`.

- **Sem otimização**: Spark lê **todos** os arquivos Parquet, decodifica **todas** as colunas de **todo** registro, então aplica o filtro e descarta colunas não usadas.
- **Com Catalyst** (predicate + projection pushdown): Spark identifica que só precisa da coluna `nome`, e que o filtro pode ser aplicado **durante a leitura** — pulando grupos de linhas/partições Parquet inteiros que não correspondem, nunca decodificando colunas que não serão usadas.

É exatamente por isso que formatos colunares como **Parquet** se tornaram o padrão do Data Lake — eles permitem que otimizadores como Catalyst pulem dados irrelevantes sem sequer lê-los do disco.

## Tungsten: Otimização em Nível de Memória e CPU

Enquanto o Catalyst otimiza **o que** executar, o **Projeto Tungsten** otimiza **como** executar em nível de hardware:

- **Gerenciamento de memória off-heap** — representa dados em formato binário compacto, evitando a sobrecarga de objetos JVM (headers, garbage collector) e reduzindo drasticamente o uso de memória.
- **Computação cache-aware** — organiza algoritmos e estruturas de dados para maximizar acertos de cache L1/L2/L3 da CPU.
- **Whole-stage code generation** — compila cadeias inteiras de operadores em um único loop de bytecode otimizado, como se um engenheiro tivesse escrito código Java especializado manualmente para aquela consulta exata.

## Lendo o Plano de Execução

```python
df.explain(True)   # plano lógico + físico, direto no terminal
```

Na aba **SQL** da interface do Spark, procure por:

- **BroadcastHashJoin** vs. **SortMergeJoin** — se uma tabela pequena está fazendo SortMerge, force `broadcast()` para eliminar o Shuffle.
- **Filter** posicionado **antes** de um **Scan** — indica que o predicate pushdown está funcionando.
- **Exchange** (= Shuffle) — todo nó Exchange no plano é um ponto de redistribuição de rede; quanto menos, melhor.
- **WholeStageCodegen** — indica que o Tungsten está compilando operações em bytecode otimizado.

## O Que Você Verá Neste Laboratório

O Lab 04 (Caso A) executa `.explain(True)` na mesma consulta como operações RDD puras vs. API DataFrame, lado a lado, para que você veja o plano que o Catalyst produz e que o caminho RDD nunca recebe.
