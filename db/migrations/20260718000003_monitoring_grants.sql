-- migrate:up

-- The CNPG metrics exporter runs custom queries under the pg_monitor role.
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pg_monitor;

-- migrate:down

ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM pg_monitor;
REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM pg_monitor;
