# Market data

## Ingester

Data will be ingested via websocket by Ingester and sent to a NATS Jetstream instance.

## Aggregator

Aggregator will watch for all data coming in via NATS and keep windowed candles up to date, sending windows to NATS KV.

## Archiver

Every hour (or so) Archiver will grab all the data stored so far in NATS, put it into a parquet file and send it using Apache Iceberg to S3.
