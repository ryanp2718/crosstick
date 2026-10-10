"""Idempotent Garage provisioning: node layout, S3 key, buckets, bronze lifecycle.

Shared by the compose `garage-init` one-shot (`python -m common.garage`) and the
integration-test fixtures, so CI provisions the store the same way the stack does.

Garage starts with no layout, no keys and no buckets, and its S3 API refuses every
request until the node holds a role. Each step here checks before it writes, so a
re-run on every `up` is a no-op. The lifecycle rule is only *seeded* when the bucket
has none, which leaves a hand-tuned retention (e.g. extended for a backfill) intact.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import os
import sys
import time
from urllib.parse import urlsplit

import httpx

# Single node, so capacity only has to be non-zero; it is not a quota.
_CAPACITY_BYTES = 1 << 40


class GarageAdmin:
    def __init__(self, admin_url: str, token: str) -> None:
        self._http = httpx.Client(
            base_url=admin_url, headers={"Authorization": f"Bearer {token}"}, timeout=10
        )

    def _get(self, path: str, **params: str) -> httpx.Response:
        return self._http.get(path, params=params)

    def _post(self, path: str, body: dict, **params: str) -> dict:
        r = self._http.post(path, json=body, params=params)
        r.raise_for_status()
        return r.json() if r.content else {}

    def wait_ready(self, timeout_s: float = 60) -> dict:
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                r = self._get("/v2/GetClusterStatus")
                r.raise_for_status()
                return r.json()
            except httpx.HTTPError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)

    def ensure_layout(self) -> None:
        status = self.wait_ready()
        layout = self._get("/v2/GetClusterLayout").json()
        if layout["roles"]:
            return
        node_id = status["nodes"][0]["id"]
        role = {"id": node_id, "zone": "local", "capacity": _CAPACITY_BYTES, "tags": []}
        self._post("/v2/UpdateClusterLayout", {"roles": [role]})
        self._post("/v2/ApplyClusterLayout", {"version": layout["version"] + 1})

    def ensure_key(self, access_key: str, secret_key: str, *, allow_create_bucket: bool) -> None:
        if self._get("/v2/GetKeyInfo", id=access_key).status_code != 200:
            body = {"accessKeyId": access_key, "secretAccessKey": secret_key, "name": "crosstick"}
            self._post("/v2/ImportKey", body)
        if allow_create_bucket:
            self._post("/v2/UpdateKey", {"allow": {"createBucket": True}}, id=access_key)

    def ensure_bucket(self, name: str, access_key: str) -> None:
        r = self._get("/v2/GetBucketInfo", globalAlias=name)
        bucket_id = r.json()["id"] if r.status_code == 200 else None
        if bucket_id is None:
            bucket_id = self._post("/v2/CreateBucket", {"globalAlias": name})["id"]
        perms = {"read": True, "write": True, "owner": True}
        body = {"bucketId": bucket_id, "accessKeyId": access_key, "permissions": perms}
        self._post("/v2/AllowBucketKey", body)


def _sigv4_request(
    method: str, url: str, body: bytes, *, access_key: str, secret_key: str, region: str
) -> httpx.Response:
    """One SigV4-signed S3 call. Only the lifecycle subresource needs it, and pyarrow's
    filesystem does not expose bucket configuration."""
    parts = urlsplit(url)
    now = dt.datetime.now(dt.UTC)
    amz_date, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers = {"host": parts.netloc, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    if body:
        headers["content-md5"] = base64.b64encode(hashlib.md5(body).digest()).decode()
    signed = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    query = "&".join(sorted(f"{q}=" if "=" not in q else q for q in parts.query.split("&") if q))
    canonical = "\n".join(
        [method, parts.path or "/", query, canonical_headers, signed, payload_hash]
    )
    scope = f"{day}/{region}/s3/aws4_request"
    to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()]
    )
    key = f"AWS4{secret_key}".encode()
    for part in (day, region, "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, "
        f"SignedHeaders={signed}, Signature={signature}"
    )
    return httpx.request(method, url, content=body, headers=headers, timeout=10)


def seed_expiry(
    s3_url: str, bucket: str, days: int, *, access_key: str, secret_key: str, region: str
) -> bool:
    """Set an expire-after-`days` rule on `bucket` if it has no lifecycle config yet.
    Returns whether a rule was written."""
    url = f"{s3_url.rstrip('/')}/{bucket}?lifecycle"
    auth = {"access_key": access_key, "secret_key": secret_key, "region": region}
    current = _sigv4_request("GET", url, b"", **auth)
    if current.status_code == 200:
        return False
    if current.status_code != 404:
        current.raise_for_status()
    xml = (
        "<LifecycleConfiguration><Rule><ID>expire-bronze</ID><Status>Enabled</Status>"
        f"<Filter></Filter><Expiration><Days>{days}</Days></Expiration>"
        "</Rule></LifecycleConfiguration>"
    ).encode()
    _sigv4_request("PUT", url, xml, **auth).raise_for_status()
    return True


def provision(
    *,
    admin_url: str,
    admin_token: str,
    access_key: str,
    secret_key: str,
    buckets: list[str],
    allow_create_bucket: bool = False,
) -> None:
    admin = GarageAdmin(admin_url, admin_token)
    admin.ensure_layout()
    admin.ensure_key(access_key, secret_key, allow_create_bucket=allow_create_bucket)
    for bucket in buckets:
        admin.ensure_bucket(bucket, access_key)


def main() -> int:
    env = os.environ
    access_key, secret_key = env["S3_ACCESS_KEY"], env["S3_SECRET_KEY"]
    provision(
        admin_url=env["GARAGE_ADMIN_URL"],
        admin_token=env["GARAGE_ADMIN_TOKEN"],
        access_key=access_key,
        secret_key=secret_key,
        buckets=["lake", "silver", "gold"],
    )
    try:
        wrote = seed_expiry(
            env["S3_ENDPOINT"],
            "lake",
            int(env.get("BRONZE_EXPIRE_DAYS", "30")),
            access_key=access_key,
            secret_key=secret_key,
            region=env.get("S3_REGION", "us-east-1"),
        )
        print("bronze lifecycle rule seeded" if wrote else "bronze lifecycle rule kept")
    except httpx.HTTPError as exc:
        # A retention hiccup must not block the materializer, which waits on this.
        print(f"WARN: bronze lifecycle rule not set: {exc}", file=sys.stderr)
    print("buckets ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
