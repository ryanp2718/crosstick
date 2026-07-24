# Runbook: Phase A cutover (silver + gold to Cloudflare R2)

Operational steps to move the derived medallion layers (silver, gold) off the local
MinIO container onto Cloudflare R2, while the bulky raw **bronze** layer stays on
MinIO. Bronze is roughly 100x the byte volume of silver + gold, so keeping it local
is the single biggest cost lever; R2's zero egress keeps serving the derived layers
free at any scale.

The code is already R2-ready and backward-compatible: with no R2 variables set, every
process resolves to MinIO and behaves byte-for-byte as before (validated with
`docker compose config`). Nothing points at R2 until the steps below are run.

## The two-endpoint model

`common.lake` exposes two filesystem helpers. Base `S3_*` is the primary/derived
endpoint; `LAKE_S3_*` is the bronze endpoint and falls back to `S3_*` when unset.

| Process | bronze | silver | gold | Endpoints |
|---|---|---|---|---|
| materializer | write | - | - | MinIO only (pinned in compose; immune to R2 vars) |
| silver batch | read | write | - | base `S3_*` = R2, `LAKE_S3_*` = MinIO |
| gold batch | - | read | write | base `S3_*` = R2 (single endpoint) |
| lake-exporter | read | read | read | base `S3_*` = R2 (RO), `LAKE_S3_*` = MinIO |

## 1. Provision R2 (one-time)

1. **Buckets** (private; no public `r2.dev` domain, no custom domain): create
   `silver` and `gold`. Do **not** create a bronze bucket; bronze never leaves MinIO.
2. **Two scoped API tokens**, each limited to the `silver` + `gold` buckets only:
   - **RW** (silver/gold batch): Object Read & Write. Fills `R2_ACCESS_KEY` /
     `R2_SECRET_KEY`.
   - **RO** (always-on exporter): Object Read only. Fills `R2_RO_ACCESS_KEY` /
     `R2_RO_SECRET_KEY`.
   Set a token TTL and, if the runner has a stable egress IP, an IP restriction.
3. **Lifecycle rule: abort incomplete multipart uploads after 1 day** on both
   buckets. pyarrow uses multipart PUT; a failed part otherwise leaks as orphaned
   storage. History is kept per decision, so add **no** object-expiration rule.
4. **Billing / usage notifications** (R2 has no hard spend cap): alerts on stored
   bytes and Class A operation volume. The exporter is designed to stay in the free
   tiers (markers are Class B GETs; the daily audit is one LIST walk), so a Class A
   spike is a signal something regressed.
5. Note the endpoint `https://<account-id>.r2.cloudflarestorage.com` and region
   `auto`. The account id is not a secret but lives only in `.env`, never in git.

## 2. Pre-flight (needs the stack up)

1. **Multipart smoke test against R2** before any real write. R2 has historically
   needed care with S3 multipart/checksum semantics:
   ```
   S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com S3_REGION=auto \
   S3_ACCESS_KEY=$R2_ACCESS_KEY S3_SECRET_KEY=$R2_SECRET_KEY S3_BUCKET=silver \
   uv run python -m scripts.r2_multipart_smoke
   ```
   Must PASS (validates multipart checksum + path/vhost addressing) before flipping.
   Uses the RW token against an existing bucket (`silver`); it writes and deletes one
   throwaway object.
2. **Measure silver + gold size** to ground the storage-cost estimate, e.g. a throwaway
   `mc du` container on the compose network (`mc du local/silver local/gold`). Storage
   is `V*I * MB_per_instrument_day * retention_days / 1024` GB at $0.015/GB-mo (10 GB
   free), linear and retention-controlled.

## 3. Migrate history (one-time)

Mirror the existing silver + gold objects, scoped to those two buckets only, never
bronze, `.env`, logs, or local dirs:
```
mc alias set r2 https://<account-id>.r2.cloudflarestorage.com $R2_ACCESS_KEY $R2_SECRET_KEY
mc mirror --overwrite local/silver r2/silver
mc mirror --overwrite local/gold   r2/gold
```
Verify object counts match (`mc ls --recursive` on both sides) and checksum-spot-check
a sample object. This is a bounded one-time Class A PUT burst.

## 4. Cut over

1. In `.env`, uncomment and fill the R2 block: `S3_ENDPOINT`, `S3_REGION=auto`,
   `R2_ACCESS_KEY` / `R2_SECRET_KEY` (RW), `R2_RO_ACCESS_KEY` / `R2_RO_SECRET_KEY` (RO).
2. **Batch (silver/gold)** run against R2 with base `S3_*` = R2 RW, and for silver also
   `LAKE_S3_*` = MinIO so its bronze reads stay local:
   ```
   S3_ENDPOINT=$S3_ENDPOINT S3_REGION=auto \
   S3_ACCESS_KEY=$R2_ACCESS_KEY S3_SECRET_KEY=$R2_SECRET_KEY \
   LAKE_S3_ENDPOINT=http://localhost:9000 \
   LAKE_S3_ACCESS_KEY=$S3_ACCESS_KEY LAKE_S3_SECRET_KEY=$S3_SECRET_KEY \
   uv run python -m silver.main <date>
   # gold needs no LAKE_S3_* (silver-in, gold-out are both R2):
   S3_ENDPOINT=$S3_ENDPOINT S3_REGION=auto \
   S3_ACCESS_KEY=$R2_ACCESS_KEY S3_SECRET_KEY=$R2_SECRET_KEY \
   uv run python -m gold.main <date>
   ```
   Run one date; verify silver/gold objects land in R2 and the scorecard/basis are
   correct. Confirm the `_freshness/<dataset>` markers were written.
3. **Exporter**: `docker compose up -d --force-recreate lake-exporter`. With the R2
   vars set, its base flips to R2 (RO) for silver/gold while bronze stays on MinIO.
   Verify `/metrics`: `lake_freshness_seconds{layer="silver"|"gold"}` come from the
   markers, and after the first daily audit `lake_freshness_marker_skew_seconds` ~ 0.
   `python ops/smoke.py` should stay green.

## 5. Rollback

Comment the R2 block back out in `.env` and `docker compose up -d --force-recreate
lake-exporter`; run the batch without the R2 env. The local MinIO stack runs untouched
throughout, and the migrated R2 objects are left in place, so rollback is immediate and
lossless.

## Security posture

- Data is public market data + derived DQ facts (prices/sizes/timestamps/latencies keyed
  by dataset/exchange/symbol/date). No PII. Spot-check a sample object's schema for host
  identifiers before the first push.
- Migration is bucket-scoped to `silver` + `gold` only; bronze, `.env`, and logs never
  leave the box.
- Private buckets, two least-privilege scoped tokens (RW batch, RO exporter), TLS via the
  `https://` endpoint scheme, region `auto`. No account id or token in any tracked file;
  real values live only in gitignored `.env`.
