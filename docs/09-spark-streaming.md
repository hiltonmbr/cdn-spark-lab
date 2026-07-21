# 🌊 Spark Structured Streaming

## Do Batch ao Streaming: A Tabela Ilimitada

Nos módulos anteriores, tudo o que fizemos foi em **lote (batch)**: um Parquet inteiro, já parado na Bronze, processado do início ao fim — uma pergunta, uma resposta final. O Structured Streaming resolve um problema diferente: dados que **nunca terminam de chegar**.

A ideia central é tratar o stream de entrada como uma **tabela ilimitada** (*Input Table*), onde cada novo dado é uma linha anexada ao final. A query que você escreve (`filter`, `groupBy`, `agg`) é a **mesma API de DataFrame do batch** — só troca `spark.read` por `spark.readStream` e `df.write` por `df.writeStream`:

```python
eventos = (
    spark.readStream
    .format("json")
    .schema(schema_eventos)     # streaming exige schema explícito — sem inferSchema
    .load("s3a://landing/eventos/")
)

alertas = eventos.filter(eventos.valor > 10000)   # mesmo DataFrame de sempre

query = alertas.writeStream.format("console").outputMode("append").start()
query.awaitTermination()
```

📌 **Por que essa analogia funciona:** o Catalyst Optimizer continua otimizando a query, `df.groupBy(...).agg(...)` continua sendo a mesma API — o motor decide **como** executar incrementalmente, você só declara **o quê**.

## Micro-Batch vs. Continuous Processing

| Característica | Micro-Batch (padrão) | Continuous Processing |
|---|---|---|
| Latência típica | ~100ms – alguns segundos | ~1ms |
| Agregações (`groupBy`, janelas) | ✅ Suportado | ❌ Não suportado |
| Joins entre streams | ✅ Suportado | ❌ Não suportado |
| Maturidade | Produção, desde 2016 | Experimental, desde 2018 |
| Caso de uso típico | Dashboards, agregações, alertas | Trading, controle industrial de altíssima frequência |

**Regra prática:** este módulo é inteiro sobre janelas temporais e watermarks — o motor **micro-batch é obrigatório** para tudo o que vem a seguir.

## Event Time vs. Processing Time

- **Event time**: o instante em que o evento **de fato aconteceu** (um campo dentro da mensagem, ex. `timestamp_venda`). É o tempo que importa para o negócio.
- **Processing time**: o instante em que o Spark **recebeu** o evento. Diverge do event time por atrasos de rede, filas, reprocessamento.

Todas as janelas deste módulo são calculadas sobre **event time** — é a única forma de produzir agregados corretos e reproduzíveis, independentemente de quando o dado efetivamente chegou.

## Tumbling Windows: Fixas e Sem Sobreposição

Divide a linha do tempo em blocos fixos e mutuamente exclusivos — cada evento pertence a **exatamente uma** janela.

```python
from pyspark.sql.functions import window, col

receita_por_janela = (
    pedidos
    .groupBy(window(col("timestamp_pedido"), "5 minutes"))
    .agg({"valor": "sum"})
)
```

Um pedido às `14:04:59` cai em `[14:00,14:05)`; um pedido às `14:05:02`, só 3 segundos depois, já cai no bloco seguinte `[14:05,14:10)` — o limite é inclusivo à esquerda, exclusivo à direita.

## Sliding Windows: Fixas e Sobrepostas

Mesma duração fixa da tumbling, mas avança em passos **menores** que sua duração — janelas consecutivas se sobrepõem, e cada evento pode contribuir para **várias** janelas ao mesmo tempo.

```python
# Janela de 10 minutos, deslizando a cada 5 → 50% de sobreposição, cada evento em 2 janelas
media_movel = (
    pedidos
    .groupBy(window(col("timestamp_pedido"), "10 minutes", "5 minutes"))
    .agg({"valor": "avg"})
)
```

O tumbling é, matematicamente, um caso particular do sliding em que `slide == duration`. Uma janela sliding "no meio" de duas janelas tumbling adjacentes soma exatamente o conteúdo das duas — é o que permite suavizar transições abruptas em métricas como médias móveis.

## Session Windows: Dinâmicas por Atividade

Ao contrário das duas anteriores — alinhadas ao relógio, com limites conhecidos de antemão —, a *session window* tem duração **variável**, definida pela atividade real de uma chave. A janela fica aberta enquanto novos eventos chegam dentro de um **gap** de inatividade; se excedido, a sessão fecha e a próxima começa do zero.

```python
from pyspark.sql.functions import session_window, col

sessoes = (
    cliques
    .withWatermark("timestamp_clique", "10 minutes")   # obrigatório para session_window
    .groupBy(col("usuario_id"), session_window(col("timestamp_clique"), "30 minutes"))
    .agg({"pagina": "count"})
)
```

⚠️ `session_window` **exige** um `withWatermark()` associado — sem ele, o Spark nunca saberia quando considerar uma sessão definitivamente encerrada, já que qualquer evento futuro poderia teoricamente reabri-la.

⚠️ `session_window` também **não suporta `outputMode("update")`** — só `complete` (estado atual, sessões abertas inclusas) e `append` (só o que já fechou). É diferente do `window()` comum (tumbling/sliding), onde `update` é o modo mais usado para acompanhar janelas em aberto.

## Watermarks: Até Quando Vale a Pena Esperar

Sem uma regra de corte, o Spark teria que manter **toda janela aberta para sempre** — só por precaução, caso um evento tardio apareça — e o estado interno cresceria sem limite. O watermark formaliza essa regra:

$$
W(t) = \max_{\text{eventos até } t}(\text{event\_time}) - \Delta
$$

**Regra de descarte:** uma janela cujo limite superior seja `≤` o watermark atual está **fechada** — o Spark emite o resultado e libera o estado. Um evento com `event_time` abaixo do watermark corrente é tarde demais e é **descartado silenciosamente**.

```python
resultado = (
    pedidos
    .withWatermark("timestamp_pedido", "10 minutes")   # tolera até 10min de atraso
    .groupBy(window(col("timestamp_pedido"), "5 minutes"))
    .agg({"valor": "sum"})
)
```

📌 **O watermark nunca recua** — mesmo que o próximo evento observado tenha um `event_time` menor (mais atrasado ainda), o watermark permanece o **máximo histórico** menos `Δ`.

## Modos de Saída

| Modo | Comportamento | Exige watermark? |
|---|---|---|
| `complete` | Reescreve a tabela de resultado inteira a cada ciclo | Não |
| `update` | Envia só as linhas que mudaram desde o último ciclo | Não (mas não suportado por `session_window`) |
| `append` | Envia só linhas que **nunca mudarão mais** | Sim — é o watermark que garante o fechamento definitivo |

**Erro comum:** `outputMode("append")` numa agregação por janela **sem** `withWatermark()` — o Spark recusa a query em tempo de análise, pois não haveria garantia de quando uma linha pode ser considerada final.

## Integração com Kafka

O conector `format("kafka")` é o padrão de fato para Structured Streaming, porque o modelo de log do Kafka (mensagens com offset imutável) viabiliza o **replay determinístico** que o Spark precisa para garantir *exactly-once* após uma falha:

```python
eventos_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-broker1:9092")
    .option("subscribe", "vendas")
    .load()
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema_vendas).alias("dados"))
    .select("dados.*")
)
```

Ler do Kafka sempre exige decodificar `value` (binário → JSON → colunas); escrever exige o caminho inverso, empacotando as colunas de volta em uma coluna `value` com `to_json(struct(...))`.

## O Que Você Verá Neste Laboratório

O `docker-compose.yml` deste projeto não inclui um broker Kafka — por isso o notebook 12 simula o stream com o **file source**: uma pasta "landing" que recebe arquivos JSON incrementalmente, lida com `readStream` exatamente como um tópico seria. Toda a lógica de janelas e watermarks é idêntica à que você usaria com `format("kafka")` — só a fonte muda.

- Reprodução literal, com dados reais processados pelo Spark, do exemplo de tumbling window do módulo teórico (R$ 400/R$ 240)
- Sliding window sobre os mesmos eventos, provando que a janela de sobreposição soma exatamente as duas janelas tumbling adjacentes
- Session windows aplicadas a "sessões de vendas" de um vendedor, com watermark fechando a sessão anterior no exato momento em que a próxima começa
- Reprodução, evento a evento, do traçado formal do watermark — incluindo um evento tardio sendo descartado na prática
- As duas queries do Exemplo 4 (`complete` e `append`) demonstrando na prática por que `session_window` não aceita `outputMode("update")`
