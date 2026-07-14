# 📂 Spark + HDFS (via YARN)

## Lendo e Escrevendo no HDFS

Spark não é um sistema de armazenamento — ele lê e escreve em fontes externas. A integração com HDFS é nativa:

```python
# Esquema RPC nativo, usado quando o Driver executa dentro do cluster
df = spark.read.text("hdfs://namenode:8020/user/data/logs.txt")
df.write.mode("overwrite").parquet("hdfs://namenode:8020/lake/gold/report/")
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

## Por Que Este Lab Usa `webhdfs://`, Não `hdfs://`

Em um deployment Spark-on-YARN tradicional, o Driver executa **dentro** do cluster (`--deploy-mode cluster`), na mesma rede do NameNode e dos DataNodes — então `hdfs://namenode:8020/...` resolve corretamente para todos os envolvidos.

Este laboratório mantém intencionalmente o Driver em **seu host** para cada caso (Caso C incluso), então `make jupyter-lab` é o mesmo comando em toda parte. Isso tem uma consequência real: seu processo Driver precisa resolver o hostname do NameNode e, para RPC nativo, o hostname de cada DataNode também (as transferências de bloco são diretas DataNode↔cliente). Contêineres Docker não são acessíveis por nome a partir do host sem configuração extra em nível de host.

A solução já presente no perfil `hadoop` do `docker-compose.yml`: um gateway **HttpFS** (serviço `proxy`, porta `14000`) que fala o protocolo REST WebHDFS e internamente faz proxy de **toda** a comunicação NameNode/DataNode — incluindo a transferência de dados, não apenas metadados. Seu Driver só precisa conhecer **um** hostname (`localhost:14000`), independentemente de quantos DataNodes existam por trás:

```python
df = spark.read.parquet("webhdfs://localhost:14000/datalake/bronze/vendas")
df.write.mode("overwrite").parquet("webhdfs://localhost:14000/datalake/silver/vendas")
```

Os Executors (executando dentro dos contêineres `nodemanager1`/`nodemanager2`, na mesma rede Docker que o gateway) usam a URL exata — Spark passa a string do caminho como está para cada worker, então não é necessário truque de esquema duplo.

> 💡 **Isto por si só é um momento de aprendizado.** A fricção que você está evitando aqui — resolução de hostname através da fronteira cliente/cluster — é precisamente por que jobs reais Spark-on-YARN em produção executam em `--deploy-mode cluster`, não `client`, conforme abordado em docs/01. Você está vendo, na prática, por que essa recomendação existe.

## Configurando Acesso ao HDFS

Em um deployment "tradicional" (Driver dentro do cluster), o Spark precisa dos arquivos de configuração do Hadoop em seu classpath:

- **`core-site.xml`** — o endereço do NameNode (ex.: `hdfs://namenode:8020`).
- **`hdfs-site.xml`** — fator de replicação, tamanho de bloco, caminhos de armazenamento.

**Na prática**: quando o Spark executa no **YARN**, estes são herdados automaticamente da instalação Hadoop. Em ambientes **cloud** (EMR, Dataproc), o provedor já configura o acesso S3/GCS. Configuração manual só é necessária para clusters **Standalone** ou **Kubernetes** conectando-se a um HDFS externo — que é exatamente a situação `webhdfs://` deste laboratório, contornada no nível do gateway em vez do nível do classpath.

## O Que Você Verá Neste Laboratório

- O Lab 08 grava o pipeline Bronze→Silver→Gold no HDFS via gateway e compara com a versão de volume compartilhado do Caso B.
- O Lab 09 percorre `spark-submit`, modos de deploy e a interface do YARN ResourceManager (`localhost:8088`) — veja o Application Master e os contêineres executor serem agendados ao vivo.
- O Lab 10 (🔥 laboratório caótico) mata um NodeManager no meio do job e observa o YARN reagendar as Tasks perdidas no sobrevivente, recomputando apenas o que foi perdido via linhagem.
