# ⚡ Do MapReduce ao Spark

## O Calcanhar de Aquiles do MapReduce

Conforme abordado no módulo de Hadoop, o MapReduce processa dados em fases rígidas: **Map → Shuffle & Sort → Reduce**. Entre cada fase, os resultados intermediários são **gravados em disco**. Um job lê do HDFS, processa e escreve de volta ao HDFS — se o próximo passo do pipeline depender desse resultado, ele lê do disco novamente.

Em algoritmos iterativos — treinar um modelo de Machine Learning, executar a mesma operação centenas de vezes sobre os mesmos dados — esse ciclo de leitura/gravação em disco se repete a cada iteração. Imagine um job de K-Means precisando de 20 iterações sobre 1 TB de dados: com MapReduce, são **20 leituras + 20 gravações** de 1 TB, mesmo com a CPU passando a maior parte do tempo ociosa esperando E/S.

## A Ideia do Spark: Memória, Não Disco

Em 2009, pesquisadores do **AMPLab** da UC Berkeley notaram que a RAM havia se tornado drasticamente mais barata e abundante desde que o MapReduce foi projetado (2004). A pergunta deles: *e se os dados intermediários permanecessem na RAM entre as etapas do pipeline, em vez de ir ao disco toda vez?*

O acesso à RAM é aproximadamente **~1000x mais rápido** que o disco (mesmo SSD): ~100 nanossegundos vs. ~100 microssegundos. Com os dados em cache na memória, a segunda iteração do K-Means não relê 1 TB do disco — ela já está na memória dos executors.

**2009** — Matei Zaharia inicia o projeto Spark como parte de sua pesquisa de PhD no AMPLab sobre workloads iterativos e interativos em cluster.
**2010** — O artigo fundamental *"Spark: Cluster Computing with Working Sets"* apresenta o **RDD**.
**2013/2014** — Doado para a Apache Software Foundation, torna-se um projeto top-level.
**2014** — Spark vence o [Daytona GraySort](https://spark.apache.org/news/spark-wins-daytona-gray-sort-100tb-benchmark.html): ordena 100 TB em 23 minutos em 206 máquinas, contra 72 minutos do Hadoop MapReduce em 2.100 máquinas.

## Spark vs. Hadoop MapReduce

| Característica | Hadoop MapReduce | Apache Spark |
|---|---|---|
| Armazenamento intermediário | Disco (entre cada fase) | **RAM** quando possível |
| Modelo de programação | Rígido: Map → Shuffle → Reduce | Dezenas de operadores encadeáveis |
| Performance batch | Linha base | **10-100x mais rápido** em workloads iterativos |
| APIs | Java (verboso) | Scala, Java, Python, R, SQL |
| Casos de uso | Somente batch | Batch, streaming, SQL, ML, gráficos — unificado |
| Tolerância a falhas | Replicação em disco | **Linhagem** (recomputação via DAG) |
| Avaliação | Eager | **Lazy** |

## Spark Não Substitui o HDFS — Ele Substitui o MapReduce

Um erro comum de iniciantes: pensar que Spark e Hadoop competem. Eles são complementares. Spark **não** é um sistema de armazenamento — ele lê e escreve em HDFS, S3, MinIO, Cassandra, JDBC, Kafka, etc. Tampouco gerencia recursos do cluster sozinho — ele executa **sobre** o YARN, Kubernetes, Mesos ou seu próprio gerenciador de cluster Standalone.

> Pense assim: HDFS (ou S3) é o **armazém**, YARN (ou Kubernetes) é o **gerenciador de recursos**, e Spark é o **motor** que processa o que está armazenado no armazém usando os recursos que o gerenciador aloca.

## Arquitetura: Driver, Executors, Cluster Manager

Spark segue o clássico modelo **Master-Worker**, também presente no HDFS (NameNode/DataNode) e no YARN (ResourceManager/NodeManager):

- **Driver Program** — o "cérebro" que coordena a execução. Cria a `SparkSession`, converte seu código em um DAG, divide em Jobs → Stages → Tasks, negocia recursos com o Cluster Manager e coleta os resultados finais.
- **Cluster Manager** — aloca CPU/RAM para a aplicação (Standalone, YARN, Kubernetes).
- **Executors** — processos workers de longa duração que executam Tasks e armazenam dados em cache. Diferente das JVMs efêmeras por tarefa do MapReduce, os executors do Spark permanecem ativos durante **toda a aplicação**, permitindo reuso em memória entre operações.

**Modos de deploy:** `spark-submit --deploy-mode client` executa o Driver na máquina que submete o job (ótimo para desenvolvimento interativo, mas a aplicação morre se essa máquina desconectar). `--deploy-mode cluster` executa o Driver dentro do próprio cluster (padrão de produção — sobrevive à desconexão da máquina que submete).

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=4g \
  --conf spark.executor.instances=10 \
  --conf spark.executor.cores=2 \
  my_job.py
```

## O Que Você Verá Neste Laboratório

Ao longo dos 4 casos, o Cluster Manager muda (`local[*]` → Standalone → Spark Connect → Standalone), mas a arquitetura Driver/Executor permanece. O Caso C agora usa **Spark Connect contra HDFS nativo** (`hdfs://namenode:8020`), sem YARN — o Spark é apenas motor, o HDFS é apenas storage.
