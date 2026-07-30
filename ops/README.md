# ops/

Operational config for the compose stack: `instruments.yml` (the canonical
instrument map every service reads), `dq_budgets.yml` (per-check data-quality
limits, see `python/gold/budget.py`), `prometheus/` (scrape config + alert
rules), `grafana/` (provisioning + dashboards), and `smoke.py`.

## Editing Prometheus/Grafana config

Prometheus and Grafana read their mounted config (`ops/prometheus/`,
`ops/grafana/`) only at container creation. After editing it, recreate rather
than restart: a restart reuses the existing container and its mounts, and a
bind of a file that did not exist at create time never materializes at all.
Then smoke-check that the new config actually loaded:

```powershell
docker compose up -d --force-recreate prometheus grafana
python ops/smoke.py   # asserts rules loaded + dashboards provisioned
```
