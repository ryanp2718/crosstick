"""Materializer: projects the Redpanda log to bronze Parquet on the lake.

Phase 1 of the analytics plan (docs/DESIGN_analytics.md). `bronze` holds the
pure projection logic; `service` wires it to a consumer + object store;
`main` is the docker-compose entrypoint.
"""
