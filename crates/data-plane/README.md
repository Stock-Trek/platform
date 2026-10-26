# Market data

## Ingester

Data will be ingested via websocket by Ingester and sent to a NATS Jetstream instance. It stores sequence numbers with the events. It also detects missing events and adds gap markers for the Backfiller.

## Backfiller

It listens for gap markers, fetches the missing data and pushes data into the same stream as Ingester.

## Aggregator

Aggregator will watch for all data coming in via NATS and keep the open candles up to date, sending updated open candles to NATS KV. When a candle is closed it gets a revision number and is sent to a Jetstream stream.

## Archiver

Archiver will be a durable Jetstream consumer, and periodically flush de-duplicated data to parquet files bucketed by exchange event timestamp and send it using Apache Iceberg to S3. It will also listen to closed candles and archive those as well.

## Compactor

Compactor will run regularly converting the raw archived S3 files into clean and tidy files, partitioning by symbol and sorting by timestamp.
