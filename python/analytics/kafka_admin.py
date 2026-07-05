"""Topic-admin helper shared by the integration harness and the offline demo.

Lives outside `tests/` because the demo entrypoint (`analytics.demo`) needs it
at runtime; test-only helpers (delete/seed) stay in `tests/kafka_admin.py`.
"""
from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from common.kafka_io import brokers_from_env


async def create_single_partition_topics(topics: list[str]) -> None:
    """Pre-create topics with one partition so replay offsets are 0..N-1 and
    per-topic ordering is deterministic. Idempotent on existing topics."""
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers_from_env())
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()
