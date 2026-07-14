# 🧩 RDDs, Linhagem e Partições

## O Que é um RDD?

O **RDD (Resilient Distributed Dataset)** é a abstração de dados original e fundamental do Spark — uma coleção imutável e distribuída de objetos, particionada entre os nós do cluster, processada em paralelo.

Quatro propriedades principais:

- **Resiliente** — recupera-se de falhas automaticamente através de *linhagem* (não réplicas).
- **Distribuído** — os dados são fatiados em **partições** espalhadas pelos Executors.
- **Imutável** — uma vez criado, um RDD nunca muda; transformações produzem **novos** RDDs.
- **Tipado e lazy** — operações só executam quando uma ação é chamada.

## Por Que Imutabilidade?

À primeira vista, nunca poder mutar dados parece limitante. Na prática, é o que torna a computação distribuída segura possível:

- **Sem condições de corrida** — múltiplos Executors podem ler o mesmo RDD simultaneamente com risco zero de um sobrescrever o que outro está lendo.
- **Cacheável com segurança** — um RDD em cache nunca "fica obsoleto", porque seu conteúdo nunca muda.
- **Linhagem confiável** — como cada RDD é gerado deterministicamente a partir do anterior, Spark pode sempre **recomputar** um RDD perdido reaplicando as mesmas transformações.

> Em vez de perguntar "como desfaço uma alteração?", o modelo do Spark pergunta "como recrio estes dados a partir de sua receita?" — essa é a essência da linhagem.

## Linhagem: Tolerância a Falhas Sem Replicação

O HDFS garante tolerância a falhas **replicando** cada bloco 3x em disco — um custo significativo de armazenamento. O Spark resolve o mesmo problema de forma completamente diferente: **lembrando a receita**.

Cada RDD mantém uma referência ao(s) RDD(s) que o produziram e à transformação aplicada (ex.: "este RDD é o resultado de aplicar `filter(age > 18)` ao RDD anterior"). Se uma partição for perdida (um Executor falhou), Spark não precisa de uma cópia de backup — ele simplesmente reexecuta a cadeia de linhagem registrada, recriando apenas a partição perdida.

Isso é fundamentalmente mais barato que replicar dados: nenhum custo extra de armazenamento durante a operação normal, apenas o custo (ocasional) de recomputação quando algo falha.

## Partições: A Unidade de Paralelismo

Assim como o HDFS fatia arquivos em **blocos** de 128 MB, o Spark fatia RDDs em **partições** — a menor unidade de trabalho que uma única Task pode processar, em um único core de um único Executor.

- Por padrão, ler um arquivo do HDFS cria **uma partição por bloco HDFS** (herdando a localidade dos dados nativamente).
- O número de partições limita o **grau máximo de paralelismo**: 100 partições permitem no máximo 100 Tasks concorrentes.
- Poucas partições → cores ociosos, paralelismo desperdiçado.
- Muitas partições → sobrecarga excessiva de agendamento de Tasks.
- Regra prática: busque **2-4 partições por core de CPU disponível**.

Hoje, a maioria das aplicações Spark usa a API **DataFrame** (abordada em docs/04) em vez de RDDs puros — mas todo DataFrame é, internamente, convertido em operações de RDD. Entender RDDs significa entender o motor por trás de tudo.

## Criando RDDs

```python
# De uma coleção Python existente na memória do Driver
rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5], numSlices=4)

# De um arquivo distribuído (HDFS, S3, local)
rdd2 = spark.sparkContext.textFile("path/to/logs.txt")

# Da transformação de outro RDD
rdd3 = rdd2.filter(lambda line: "ERROR" in line)

print(rdd1.getNumPartitions())  # 4
print(rdd3.count())              # dispara a execução (uma action)
```

## O Que Você Verá Neste Laboratório

O Lab 04 (Caso A, local) torna a linhagem tangível: você construirá uma pequena cadeia de RDDs, inspecionará `.toDebugString()` para ver o grafo de linhagem que o Spark registrou, e comparará com o plano equivalente do DataFrame a partir de `.explain()` no mesmo notebook.
