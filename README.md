# PostgreSQL Real-time Storage Monitor Daemon

A lightweight daemon to **enforce per-user storage limits in PostgreSQL** with real-time response.  
It uses PostgreSQL's `LISTEN/NOTIFY` mechanism combined with triggers to detect DML operations,  
checks storage usage, and if a user exceeds the **hard limit**, their account is locked and **all active sessions are terminated immediately**.

---

## Features

- **Real-time monitoring** (event-driven, not polling)
- **Soft & Hard limits** per user, configurable via JSON
- **Immediate account lock** + **session termination** when hard limit exceeded
- **Prometheus exporter** for metrics visualization
- **Slack/Webhook notifications**
- **Syslog logging** for system integration
- **PID lock** to prevent multiple instances
- Graceful shutdown via `SIGTERM`/`SIGINT`

---

## Configuration

### 1. User limits (`/etc/postgres_monitor/user_limits.json`)

```json
{
  "user_limits": [
    {
      "user": "dev_user",
      "soft_limit": 5000000000,
      "hard_limit": 10000000000,
      "manual_unlock": false
    },
    {
      "user": "analytics_user",
      "soft_limit": 20000000000,
      "hard_limit": 30000000000,
      "manual_unlock": true
    }
  ]
}
```


### 2. Monitor config (/etc/postgres_monitor/monitor_config.json)

```json
{
  "slack_enabled": false,
  "slack_webhook_url": "",
  "prometheus_enabled": true
}
```

### 3. DB password (/root/etc/postgres_monitor/monitor_user.passwd)

```
SuperSecretPassword
```

### PostgreSQL Setup
```sql
-- Create monitoring user
CREATE USER monitor_user WITH PASSWORD 'SuperSecretPassword';

-- Grant catalog access
GRANT SELECT ON pg_class TO monitor_user;
GRANT SELECT ON pg_namespace TO monitor_user;
GRANT SELECT ON pg_roles TO monitor_user;
GRANT SELECT ON pg_stat_activity TO monitor_user;

-- Allow terminating other sessions
GRANT pg_signal_backend TO monitor_user;

-- Allow role locking/unlocking
ALTER ROLE monitor_user CREATEROLE;
```
Apply the provided SQL trigger script:
```sh
psql -U postgres -d mydb -f setup_realtime_triggers.sql
```

### Build and Install
```sh
# Build the daemon
gcc -O2 -Wall -o postgres_realtime_monitor_daemon postgres_realtime_monitor_daemon.c \
    -lpq -lcurl

# Deploy
sudo cp postgres_realtime_monitor_daemon /usr/local/bin/
sudo chown postgres:postgres /usr/local/bin/postgres_realtime_monitor_daemon
sudo chmod 750 /usr/local/bin/postgres_realtime_monitor_daemon
```

### Systemd Service
Create /etc/systemd/system/postgres_realtime_monitor.service:
```sh
[Unit]
Description=PostgreSQL Real-time Storage Monitor Daemon
After=network.target postgresql.service

[Service]
Type=simple
ExecStart=/usr/local/bin/postgres_realtime_monitor_daemon
User=postgres
Group=postgres
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```
Enable and start:
```sh
sudo systemctl daemon-reload
sudo systemctl enable postgres_realtime_monitor.service
sudo systemctl start postgres_realtime_monitor.service
```

### Prometheus Metrics
If enabled in config, metrics are written to:
```sh
/var/lib/prometheus/node-exporter/postgres_realtime_monitor.prom
```
Example:
```sh
# HELP postgres_user_storage_usage_bytes Current storage usage of a user in bytes.
# TYPE postgres_user_storage_usage_bytes gauge
postgres_user_storage_usage_bytes{user="dev_user"} 10485760
postgres_user_storage_usage_bytes{user="analytics_user"} 52428800
```
Alerts

 - Soft limit exceeded → Syslog warning + Slack notification
 - Hard limit exceeded →
   - Active sessions terminated (pg_terminate_backend)
   - Account locked (ALTER ROLE ... NOLOGIN)

Syslog critical entry + Slack alert

### License
MIT License
