# Grafana dashboards

Source of truth for the twstats Grafana dashboards. Deployed via the flux
repo (`infrastructure/monitoring/grafana/dashboards/twstats.yaml`) as a
ConfigMap — when editing here, copy the (compacted) JSON into that ConfigMap.

- `pipeline-dashboard.json` — pipeline overview: live streams, freshness,
  archive backlog, daily raw/archive chunk volume, kafka consumer lag,
  database size, cronjob heartbeats, backups.
