"""An ephemeral, provisioned Garage for the S3 integration tests (testcontainers)."""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pyarrow import fs as pafs

from common.garage import provision

GARAGE_IMAGE = "dxflrs/garage:v2.4.1"
CONFIG = Path(__file__).resolve().parents[3] / "ops" / "garage" / "garage.toml"


@contextmanager
def garage_filesystem() -> Iterator[pafs.S3FileSystem]:
    """Yield a filesystem on a fresh single-node Garage whose key may create buckets,
    so tests write to whatever bucket names they like, as they did on MinIO."""
    from testcontainers.core.container import DockerContainer

    admin_token = secrets.token_hex(16)
    access_key, secret_key = "GK" + secrets.token_hex(12), secrets.token_hex(32)
    container = (
        DockerContainer(GARAGE_IMAGE)
        .with_env("GARAGE_RPC_SECRET", secrets.token_hex(32))
        .with_env("GARAGE_ADMIN_TOKEN", admin_token)
        .with_volume_mapping(str(CONFIG), "/etc/garage.toml", "ro")
        .with_exposed_ports(3900, 3903)
    )
    with container:
        host = container.get_container_host_ip()
        provision(
            admin_url=f"http://{host}:{container.get_exposed_port(3903)}",
            admin_token=admin_token,
            access_key=access_key,
            secret_key=secret_key,
            buckets=[],
            allow_create_bucket=True,
        )
        yield pafs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=f"http://{host}:{container.get_exposed_port(3900)}",
            scheme="http",
            region="us-east-1",
            allow_bucket_creation=True,
        )
