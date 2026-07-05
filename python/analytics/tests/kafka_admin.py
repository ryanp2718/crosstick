"""Test-only admin helpers for the integration harness (ephemeral Redpanda).

Kept separate from conftest fixtures so both replay and gateway integration
tests can reuse them. Topic creation lives in `analytics.kafka_admin` (shared
with the offline demo).
"""
from __future__ import annotations

import asyncio

from aiokafka.admin import AIOKafkaAdminClient

from common.kafka_io import brokers_from_env


async def delete_topics(topics: list[str]) -> None:
    """Delete whichever of `topics` exist and wait until the broker forgets
    them, so an immediate re-create can't race the asynchronous deletion.

    The md.* topic names are fixed by contract, so tests that need a clean log
    (fresh offsets, no prior test's records) reset topics rather than rename.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers_from_env())
    await admin.start()
    try:
        targets = set(topics)
        existing = targets & set(await admin.list_topics())
        if existing:
            await admin.delete_topics(list(existing))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10.0
        while leftover := targets & set(await admin.list_topics()):
            if loop.time() > deadline:
                raise TimeoutError(f"topics still present after delete: {sorted(leftover)}")
            await asyncio.sleep(0.1)
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
