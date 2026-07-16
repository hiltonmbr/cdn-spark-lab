# 📂 Spark + HDFS (via Spark Connect)

## Lendo e Escrevendo no HDFS

Spark não é um sistema de armazenamento — ele lê e escreve em fontes externas. A integração com HDFS é nativa:

```python
# Esquema RPC nativo, usado pelo Spark Connect dentro da rede Docker
df = spark.read.parquet("hdfs://namenode:8020/datalake/bronze/vendas")
df.write.mode("overwrite").parquet("hdfs://namenode:8020/datalake/gold/vendas_por_setor/")
```

**Atenção**: o diretório de saída não deve **já existir** (a menos que você use `.mode("overwrite")`). Spark grava saída particionada — o "arquivo de saída" é na verdade um **diretório** contendo múltiplos `part-00000`, `part-00001`, etc., um por partição.

## Formatos de Arquivo: Qual Escolher?

| Formato | Tipo | Compressão | Leitura parcial | Quando usar |
|---|---|---|---|---|
| **Parquet** | Colunar | Snappy/GZIP | ✅ Colunas + partições | **Padrão para Data Lake** — melhor ajuste para pushdown do Catalyst |
| **ORC** | Colunar | ZLIB/Snappy | ✅ Colunas + partições | Alternativa ao Parquet, popular no ecossistema Hive |
| **Avro** | Linha | Deflate/Snappy | ❌ Lê tudo | Ideal para **streaming** e schemas que evoluem com frequência |
| **CSV/JSON** | Texto | Opcional | ❌ Lê tudo | Interoperabilidade, mas **ineficiente** para Big Data |
| **Delta Lake** | Colunar+ACID | Snappy | ✅ Colunas + partições | Parquet com transações ACID e time travel |

**Regra prática**: para pipelines de Big Data, **Parquet é o padrão de facto** — o mesmo formato usado nos módulos de Hadoop e Object Storage nas camadas Silver/Gold. Ele permite que o Catalyst faça predicate e projection pushdown, lendo apenas as colunas e partições realmente necessárias.

## Arquitetura: Spark Connect + HDFS (sem YARN)

O Caso C agora usa **Spark Connect** contra um cluster **HDFS puro** — sem YARN, sem `host.docker.internal`, sem `config/hadoop-client/`.

```
┌──────────────────────────────────────────────────────┐
│                     labnet (Docker)                    │
│                                                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │ namenode │  │ datanode1│  │ datanode2│            │
│  │ :8020    │  │          │  │          │            │
│  └────┬─────┘  └──────────┘  └──────────┘            │
│       │          HDFS cluster                          │
│  ┌────▼─────┐                                        │
│  │  proxy   │  HttpFS (upload de dados do host)      │
│  │ :14000   │                                        │
│  └──────────┘                                        │
│                                                        │
│  ┌─────────────────────────────────┐                  │
│  │  Spark Standalone Cluster       │                  │
│  │  ┌──────────┐  ┌──────────────┐ │                  │
│  │  │ master   │  │ spark-connect│ │  hdfs://         │
│  │  │ :7077    │  │ :15004  ────────► namenode:8020   │
│  │  └──────────┘  └──────────────┘ │                  │
│  │  ┌──────────┐ ┌──────────────┐  │                  │
│  │  │ worker-1 │ │   worker-2   │  │                  │
│  │  └──────────┘ └──────────────┘  │                  │
│  └─────────────────────────────────┘                  │
└──────────────────────────────────────────────────────┘
         ▲                           ▲
         │ http://localhost:14000    │ sc://localhost:15004
         │ (upload via hdfs lib)     │ (Spark Connect)
         │                           │
    ┌────┴───────────────────────────┴────┐
    │         Jupyter (host)               │
    │   notebook 11: Spark + HDFS          │
    └────────────────────────────────────┘
```

**Principais diferenças do Caso C antigo (YARN):**

| Aspecto | Antigo (YARN) | Novo (Spark Connect) |
|---------|---------------|---------------------|
| Spark driver | No HOST (client mode) | No container (Spark Connect server) |
| Gerenciador de recursos | YARN (ResourceManager + NodeManagers) | Spark Standalone (master + workers) |
| Acesso HDFS | `webhdfs://localhost:14000` (via HttpFS) | `hdfs://namenode:8020` (nativo RPC) |
| Config do host | `config/hadoop-client/` | Nenhuma (não precisa) |
| Network trick | `host.docker.internal` | Nenhum (tudo na mesma rede Docker) |
| Upload de dados | `upload_bronze_table_to_hdfs()` | Mesmo — via HttpFS `localhost:14000` |

## Por Que `hdfs://` Agora Funciona

No Caso C antigo, o Spark driver executava no HOST e os executores dentro de containers. O driver não conseguia resolver `hdfs://namenode:8020` porque:
1. O hostname `namenode` não era resolvível a partir do host
2. As conexões de dados (DataNode↔cliente) precisavam de `host.docker.internal`

A solução foi usar `webhdfs://localhost:14000` — o gateway HttpFS como intermediário.

No novo Caso C, **tudo executa dentro da rede Docker**:
- O Spark Connect server é um container na mesma rede do HDFS
- Os workers também estão na mesma rede
- `namenode:8020` é resolvível via Docker DNS
- As conexões de dados (DataNode↔worker) funcionam porque `dfs.client.use.datanode.hostname=true` e `datanode1`/`datanode2` são resolvíveis via Docker DNS

Portanto, `hdfs://namenode:8020` funciona sem truques.

## HttpFS: Só para Upload

O gateway HttpFS (serviço `proxy`, porta `14000`) ainda existe, mas seu uso é restrito ao **upload inicial de dados** do host para o HDFS:

```python
from hdfs import InsecureClient
client = InsecureClient("http://localhost:14000", user="root")
client.write("/datalake/bronze/vendas/ano=2024/mes=1/part-000.parquet", data, overwrite=True)
```

Uma vez que os dados estão no HDFS, todo o processamento Spark usa `hdfs://namenode:8020` diretamente.

## hdfs:// vs webhdfs:// na Prática

| | `hdfs://namenode:8020` | `webhdfs://proxy:14000` |
|---|---|---|
| Protocolo | RPC nativo (binário) | HTTP/REST |
| Performance | Máxima (I/O direto) | Menor (overhead HTTP) |
| Resolução de hostname | Precisa de DNS Docker | Apenas `proxy` |
| Ideal para | Processamento Spark | Upload / debug |

No notebook 11, você verá **ambos os protocolos** funcionando — e o plano Catalyst é idêntico para os dois. Isso prova que o Spark abstrai completamente o sistema de arquivos.

## O Que Você Verá Neste Laboratório

- Notebook 11 — pipeline completo Bronze→Silver→Gold no HDFS via Spark Connect
- Upload de dados via HttpFS + processamento via `hdfs://` nativo
- Comparação entre `hdfs://` e `webhdfs://` — mesmos dados, mesmo resultado
- Comparação HDFS × S3 — mesmo notebook, dois storages diferentes
