# ☁️ Spark + Armazenamento de Objetos (S3 / RustFS)

## Spark com Armazenamento Compatível com S3

Conforme abordado no módulo de Object Storage, a tendência moderna é o armazenamento compatível com S3 (AWS S3, RustFS, MinIO) em vez de HDFS. O Spark se conecta a qualquer um deles via protocolo `s3a://` — o conector Hadoop-AWS:

```python
df = spark.read.parquet("s3a://silver/vendas/")
df.write.mode("overwrite").parquet("s3a://gold/relatorio/")
```

**O mesmo código funciona contra AWS S3, RustFS e MinIO** — apenas a configuração do endpoint muda. Essa é a verdadeira vantagem do S3 como protocolo aberto: o Spark não precisa saber se está falando com a AWS ou um servidor on-premises.

## Configurando o Conector S3

```python
spark = SparkSession.builder \
    .appName("SparkWithS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs-server:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "adminpassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

**`path.style.access = true` é obrigatório** para servidores on-premises (RustFS, MinIO). O AWS S3 usa por padrão o *virtual-hosted style*, enquanto soluções on-premises usam *path style* — esta flag é a diferença entre funcionar e falhar silenciosamente.

Neste laboratório, essa configuração já está no comando de inicialização do serviço `spark-connect` no `docker-compose.yml` — seu notebook, conectando-se como um cliente Spark Connect leve, não precisa repeti-la.

## S3 vs. HDFS no Spark: O Que Muda?

| Aspecto | HDFS | Object Storage (S3) |
|---|---|---|
| Esquema URI | `hdfs://namenode:8020/...` | `s3a://bucket/prefixo/...` |
| Localidade dos dados | ✅ Tasks executam perto dos dados | ❌ Dados vêm pela rede (sem localidade) |
| Renomeação atômica | ✅ Rápida (metadado do NameNode) | ⚠️ Lenta (copia + deleta) — impacta commits |
| Consistência | ✅ Imediata | ✅ Eventual → forte (S3 desde 2020) |
| Escalabilidade | Limitada pelo NameNode | Virtualmente ilimitada |
| Custo | Cluster dedicado 24/7 | Pague apenas pelo que armazenar |

**Dica de performance**: como o S3 não tem localidade de dados, o gargalo se desloca para a **rede, não o disco**. Formatos colunares (Parquet) e **particionamento de colunas** (ex.: `ano=2026/mes=07/`) são ainda mais importantes aqui do que no HDFS — são o que permite ao Spark pular a leitura de dados que não precisa.

## Por Que Commits em Object Storage São Mais Lentos

Sistemas de arquivos tradicionais (incluindo HDFS) implementam `rename()` como uma operação de metadado O(1) — trocar um ponteiro. Object stores não têm um rename nativo: renomear um "arquivo" significa **copiar** cada byte para a nova chave, então deletar a antiga. O protocolo de commit padrão do Spark depende de renomeações para publicar atomicamente a saída do job, então no S3 isso pode se tornar um custo significativo em gravações grandes — uma das razões pelas quais formatos como Delta Lake e Iceberg existem (eles substituem commits baseados em rename por um log de transações).

## O Que Você Verá Neste Laboratório

- O Lab 11 conecta-se ao RustFS via `s3a://` e executa o mesmo pipeline Bronze→Silver→Gold dos Casos B e C, escrevendo nos buckets `bronze`/`silver`/`gold` criados por `make up-s3`.
- Os notebooks 10 (Spark Connect + RustFS S3) e 11 (Spark Connect + HDFS, em notebook separado) executam o mesmo pipeline contra dois storages diferentes — S3 (object store) e HDFS (filesystem) — demonstrando que o Spark abstrai completamente o sistema de armazenamento.
