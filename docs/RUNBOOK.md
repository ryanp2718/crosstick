# Operations runbook

Day-to-day operation of the crosstick stack and the recurring host-level hazards
it runs into. The platform is a single `docker compose` stack on one machine
(Windows 11 host, Docker Desktop on WSL2), so most of the sharp edges below are
Windows/WSL2/Hyper-V specific rather than application bugs. For the one-time
storage migration of the derived layers to Cloudflare R2, see
[RUNBOOK_r2_cutover.md](RUNBOOK_r2_cutover.md).

## The stack

Bring it up with `docker compose up -d --wait`; healthchecks gate readiness, so
the command blocks until services are actually serving rather than merely
created. User-facing endpoints:

| Service | Port | Purpose |
|---|---|---|
| gateway | 8080 | WebSocket feed + dashboard + `/metrics` |
| redpanda | 19092 | external Kafka API |
| redpanda-console | 8090 | broker and topic UI |
| prometheus | 9090 | metrics and alerts |
| grafana | 3000 | dashboards (bound to localhost) |
| minio | 9000 | S3 API (bound to localhost) |

The per-exchange ingesters and the lake-exporter expose Prometheus metrics on the
91xx range. Redpanda runs single-shard (`--smp=1`) with a hard `--memory=1536M`
cap; that cap is load-bearing, see the Docker flap below.

### Config-change rule (Prometheus / Grafana / alerts)

After editing anything under `ops/`, recreate the affected service, never restart
it:

```
docker compose up -d --force-recreate prometheus grafana
```

Bind-mounted config is only re-read on container (re)creation, and a bind of a
file that does not yet exist mounts as an empty directory instead of failing
loudly. `restart` reuses the old config and hides the edit. Verify with
`python ops/smoke.py`, which checks the running config against the tree and
prints this exact command when it finds drift. See
[../ops/README.md](../ops/README.md).

## Recurring hazards

Symptom first, since that is how you meet them.

### Container create fails with 500/502

- **Symptom:** `docker compose up` or any new container create returns a 500/502
  from the Docker engine; already-running containers are fine.
- **Cause:** Redpanda's Seastar allocator reserves memory greedily. Left
  uncapped it held roughly 77% of the WSL2 VM, leaving no headroom for the engine
  to start a new container.
- **Guard in place:** redpanda is pinned to `--memory=1536M` in
  `docker-compose.yml`, which returned several GiB of VM headroom.
- **If it recurs:** restart Docker Desktop to reclaim the VM, then bring the
  stack back up. Do not retry-loop the `up`; the retries only thrash a VM that
  has no memory to give. Check free memory inside WSL2 if it keeps happening.

### A published port is dead after a host reboot

- **Symptom:** a published port (commonly 19092, 8080, 9090, 3000) refuses
  connections after a Windows reboot even though the container is healthy.
- **Cause:** Hyper-V reserves dynamic TCP port-exclusion ranges that reshuffle on
  every reboot. When a range lands on top of a published port, Docker cannot bind
  it and the mapping is silently dead.
- **Action:** after any reboot, list the exclusions and confirm none cover the
  stack's ports:
  ```
  netsh interface ipv4 show excludedportrange protocol=tcp
  ```
  If a stack port falls inside an excluded range, restart the Host Network
  Service (`net stop hns && net start hns`, elevated) to reshuffle the ranges, or
  remap the service to a free port. Tooling that only needs broker access can
  sidestep a dead external 19092 with a throwaway container on the compose
  network, talking to `redpanda:9092` internally.

### Recv-time latencies show a step / recv-clock canary trips

- **Symptom:** ingest-side recv-time latencies jump by about 2s and the
  `md_recv_clock_*` canary fires. Exchange-stamped times are unaffected.
- **Cause:** the WSL2 guest clock steps (about 1.9s) across a host sleep/resume.
- **Guard in place:** the guest clocksource is pinned to
  `hyperv_clocksource_tsc_page` via `.wslconfig` `kernelCommandLine`, which holds
  zero steps while the host stays awake.
- **Action:** keep the host awake during any capture window (set the DC and AC
  standby timeouts to 0). A sleep/resume still costs one step, so treat any window
  spanning a resume as suspect and mask it using the DQ recv-clock facts rather
  than trusting recv-time latencies through it.

## Gateway warm-start

On restart the gateway re-derives every book from the log rather than warming off
the live edge, so a cold start after downtime is a catch-up burst, not an instant
ready. Under sustained catch-up it runs near one core with bounded, draining lag;
that is expected load, not a regression. The kafkajs session timeout is raised
well above the client default so a slow join is not evicted by the single-shard
coordinator mid-warm-start (see `node/gateway/src/server.ts`). Wait for the
warm-start plan line in the gateway log before expecting a complete NBBO.
