# Services

- query-api: Continuously Running. This is the web server (Axum/Tonic) that sits behind your load balancer to listen for incoming SQL queries, process them via DataFusion, and stream back results instantly.
- catalog-cache: Continuously Running. This contains the background worker loop (tokio::spawn) that runs indefinitely to poll AWS Glue for updates and keep your in-memory metadata warm.
- datafusion-core: Stateless Library Crate. This is a shared internal library module containing your configuration logic, S3 object store registrations, and caching layer definitions. It is compiled into the other running services.
