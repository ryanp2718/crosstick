"""Admin helpers shared by the integration harness (ephemeral Redpanda).

Kept separate from conftest fixtures so both replay and gateway integration
tests can reuse them.
"""
from __future__ import annotations

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from common.kafka_io import brokers_from_env


async def create_single_partition_topics(topics: list[str]) -> None:
    """Pre-create topics with one partition so replay offsets are 0..N-1 and
    per-topic ordering is deterministic."""
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


async def seed_group_offsets(group_id: str, topics: list[str], offset: int = 0) -> None:
    """Commit `offset` for partition 0 of each topic under `group_id`.

    The gateway consumer subscribes with `fromBeginning: false`; pre-seeding a
    committed offset makes it resume from `offset` rather than skip to latest,
    so a replay-then-start flow reads every replayed record (no startup race).
    """
    from aiokafka import AIOKafkaConsumer
    from aiokafka.structs import OffsetAndMetadata, TopicPartition

    consumer = AIOKafkaConsumer(
        bootstrap_servers=brokers_from_env(),
        group_id=group_id,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        tps = [TopicPartition(t, 0) for t in topics]
        consumer.assign(tps)
        await consumer.commit({tp: OffsetAndMetadata(offset, "") for tp in tps})
    finally:
        await consumer.stop()
