# Runbook: migrate the lake from MinIO to Garage (one-time)

MinIO no longer publishes pullable images: anonymous pulls of `minio/minio` and
`minio/mc` fail on both Docker Hub and quay.io. The stack now runs
[Garage](https://garagehq.deuxfleurs.fr/) as its S3 store. This runbook moves an
existing install's `lake`, `silver` and `gold` buckets across.

The old MinIO can still be *run*, only not *pulled*. Before starting, confirm the
image is cached (`docker images minio/minio`) and do not `docker image prune -a`,
`docker system prune` or `docker compose pull` until step 6 is done. An offline copy
restores with `docker load -i <tarball>` if the cache is gone.

The source is only ever read, so every step before 6 is reversible.

## 1. Credentials

Add the Garage secrets to `.env` (see `.env.example` for the shapes). Keep the old
`MINIO_ROOT_*` lines until step 6; they are the copy's source credentials.

```
GARAGE_RPC_SECRET=$(openssl rand -hex 32)
GARAGE_ADMIN_TOKEN=$(openssl rand -hex 16)
S3_ACCESS_KEY=GK$(openssl rand -hex 12)
S3_SECRET_KEY=$(openssl rand -hex 32)
```

## 2. Start Garage beside MinIO

```
docker compose up -d garage garage-init
docker compose logs garage-init      # expect "buckets ready"
```

The running `minio` container is now an orphan of the compose project and keeps
serving; do not pass `--remove-orphans`.

## 3. Quiesce the lake

```
docker compose stop materializer lake-exporter
```

Make sure no silver/gold batch is running on the host. The ingesters keep writing to
Redpanda, and the materializer resumes from its committed offsets in step 5, so no
bronze is lost as long as the cutover finishes inside the log retention (3 days).

## 4. Copy and verify

Define the two remotes once, as env vars on a throwaway rclone container on the
compose network:

```
set -a; . ./.env; set +a
RC="docker run --rm --network crosstick_default \
 -e RCLONE_CONFIG_SRC_TYPE=s3 -e RCLONE_CONFIG_SRC_PROVIDER=Minio \
 -e RCLONE_CONFIG_SRC_ENDPOINT=http://minio:9000 \
 -e RCLONE_CONFIG_SRC_ACCESS_KEY_ID=$MINIO_ROOT_USER \
 -e RCLONE_CONFIG_SRC_SECRET_ACCESS_KEY=$MINIO_ROOT_PASSWORD \
 -e RCLONE_CONFIG_DST_TYPE=s3 -e RCLONE_CONFIG_DST_PROVIDER=Other \
 -e RCLONE_CONFIG_DST_ENDPOINT=http://garage:3900 -e RCLONE_CONFIG_DST_REGION=us-east-1 \
 -e RCLONE_CONFIG_DST_ACCESS_KEY_ID=$S3_ACCESS_KEY \
 -e RCLONE_CONFIG_DST_SECRET_ACCESS_KEY=$S3_SECRET_KEY \
 rclone/rclone:1.75.2"

for b in lake silver gold; do
  $RC copy src:$b dst:$b --transfers 4
  $RC check src:$b dst:$b --download   # expect "0 differences found"
done
```

`check --download` compares bytes, not ETags; ETags differ for multipart objects
between the two stores. A rehearsal moved `gold` (28 objects, 550 MiB) at about
150 MiB/s and found 0 differences.

The app key cannot create buckets, so a typo in a bucket name fails with a 403 rather
than silently making a new one.

## 5. Bring the lake services back on Garage

```
docker compose up -d --build materializer lake-exporter
```

Confirm the materializer flushes new bronze objects into Garage, and that
`lake_freshness_seconds` on the exporter's `/metrics` looks as it did before.

## 6. Retire MinIO

Once a few days of new bronze have landed and silver/gold have been rebuilt at least
once from Garage:

```
docker rm -f minio minio-init
docker volume rm crosstick_minio-data
```

Then delete `MINIO_ROOT_*` from `.env`.

## Notes

- **Bronze expiry restarts.** Garage stamps each copied object with the copy time, so
  migrated bronze will be kept for up to 30 days *after the migration*, not 30 days
  after it was written. `garage-init` seeds the 30-day rule on the new `lake` bucket.
- **Rollback** (any time before step 6): `git checkout` the previous
  `docker-compose.yml`, restore the old `S3_*` values in `.env`, and
  `docker compose up -d`. MinIO and its volume were never written to.
