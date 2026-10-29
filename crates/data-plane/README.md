# Market data

## Ingester

Data will be ingested via websocket by Ingester and sent to a NATS Jetstream instance. It stores sequence numbers with the events. It also detects missing events and adds gap markers for the Backfiller.

## Backfiller

It listens for gap markers, fetches the missing data and pushes data into the same stream as Ingester.

## Aggregator

Aggregator consumes both market events and gap markers from NATS. A router hashes each exchange and trading pair onto a shard subject, and one partition writer owns every candle key on that shard. Gap markers close candle windows that ended before the gap was observed.

Each partition writer keeps the open candles up to date, sends updated open candles to NATS KV, and publishes closed candles to a Jetstream stream with a revision number. Source positions are checkpointed before a message is acknowledged, so a restarted partition resumes from the first unprocessed message.

## Archiver

Archiver will be a durable Jetstream consumer, and periodically flush de-duplicated data to parquet files bucketed by exchange event timestamp and send it using Apache Iceberg to S3. It will also listen to closed candles and archive those as well.

## Compactor

Compactor will run regularly converting the raw archived S3 files into clean and tidy files, partitioning by symbol and sorting by timestamp.

## Rebuilder

Rebuilder replays archived market events from Iceberg to deterministically regenerate candles after an aggregator configuration or code change. It prefers compacted event partitions, which are already de-duplicated and sorted by exchange stream, timestamp, and sequence, and falls back to raw partitions when no compacted table exists. Events are grouped by stream and replayed through the aggregator's pure candle processor using a new state epoch, then written as raw candle Parquet objects and registered in the raw candle Iceberg table.
