# PostgreSQL Real-time Storage Monitor

This project provides a **real-time PostgreSQL user storage monitoring system**.  
It uses PostgreSQL event triggers and a daemon process to enforce per-user storage quotas.

## Features

- **Real-time quota enforcement** via `LISTEN/NOTIFY`
- **Soft/Hard limits** per user
- **Session termination & account locking** when limits are exceeded
- **Prometheus metrics** export
- **Slack notifications** (optional)
- **Flexible modes**:
  - `enterprise_db`: for a few heavy users with many tables (short cache TTL, large batch termination)
  - `shared_hosting`: for tens of thousands of lightweight users (longer cache TTL, small batch termination)

---

## Components

### 1. SQL Setup Script (`setup_realtime_triggers.sql`)

- Creates:
  - `user_limits` table (holds monitored users and quota values)
  - `storage_monitor_notify()` function
  - Event trigger for `CREATE/ALTER TABLE` and `CREATE SCHEMA`
- Attaches DML triggers to tables owned by monitored users

### 2. User Limits (`user_limits.json` and `user_limits` table)

- **JSON file**: canonical source of user quotas
- **Postgres table (`user_limits`)**: internal copy, used by event triggers

Example `user_limits.json`:

```json
{
  "user_limits": [
    {
      "user": "dev_user",
      "soft_limit": 5000000000,
      "hard_limit": 10000000000,
      "hard_grace_period": 600,
      "unlock_grace_period": 300,
      "manual_unlock": false
    },
    {
      "user": "analytics_user",
      "soft_limit": 20000000000,
      "hard_limit": 30000000000,
      "hard_grace_period": 1200,
      "unlock_grace_period": 600,
      "manual_unlock": true
    }
  ]
}
```

### 3. Daemon (postgres_realtime_monitor_daemon.c)

Runs as a background service

Connects to Postgres and LISTENs for storage_limit_check notifications

Reads user_limits.json for enforcement

Configurable via /etc/postgres_monitor/monitor_config.json

Example monitor_config.json:
```
{
  "prometheus_enabled": true,
  "slack_enabled": false,
  "slack_webhook_url": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
  "mode": "shared_hosting",
  "cache_ttl_seconds": 15,
  "session_batch_size": 100
}
```

## Installation & Setup
### 1. Apply SQL Setup
psql -U postgres -d postgres -f setup_realtime_triggers.sql

2. Sync user_limits.json → Postgres table

Convert JSON to CSV with jq:
```bash
jq -r '.user_limits[] |
  [.user, .soft_limit, .hard_limit, .hard_grace_period, .unlock_grace_period, .manual_unlock] |
  @csv' /etc/postgres_monitor/user_limits.json > /tmp/user_limits.csv
```

Load into Postgres:
```bash
psql -U postgres -d postgres -c "\copy user_limits FROM '/tmp/user_limits.csv' CSV"
```

Automate with cron/systemd timer for regular sync.

### 3. Compile and Install Daemon
```
gcc -o postgres_realtime_monitor_daemon_flex postgres_realtime_monitor_daemon.c -lpq -lcurl
sudo install -m 755 postgres_realtime_monitor_daemon_flex /usr/local/postgres_realtime_monitor_daemon/

### 4. Configure systemd service

Example /etc/systemd/system/postgres_realtime_monitor.service:

```bash
[Unit]
Description=PostgreSQL Realtime Storage Monitor Daemon
After=network.target postgresql.service

[Service]
ExecStart=/usr/local/bin/postgres_realtime_monitor_daemon_flex
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
systemctl enable postgres_realtime_monitor
systemctl start postgres_realtime_monitor
```

## Usage Examples

### Case 1: Enterprise DB (a few heavy users)

Mode: enterprise_db
#### Recommended config:

```json
{
  "mode": "enterprise_db",
  "cache_ttl_seconds": 2,
  "session_batch_size": 500
}
```

- Behavior:

 - Very frequent storage checks (almost real-time)
 - Larger batch session termination (efficient for fewer users)

### Case 2: Shared Hosting (tens of thousands of users)

Mode: shared_hosting

#### Recommended config:
```json
{
  "mode": "shared_hosting",
  "cache_ttl_seconds": 15,
  "session_batch_size": 100
}
```

 - Behavior:
  - Cache results longer to reduce load
  - Kill sessions in small batches to avoid spikes
  - Scales to tens of thousands of roles

#### Monitoring

Prometheus metrics written to:
```bash
/var/lib/prometheus/node-exporter/postgres_realtime_monitor.prom
```

#### Example metric:
```bash
postgres_user_storage_usage_bytes{user="dev_user"} 12345678
```

### Notes

- Keep user_limits.json as the single source of truth.
- Sync JSON → Postgres regularly (cron/systemd timer).
- Ensure Postgres postgres user can read the JSON file if needed.
- Always test in staging before production deployment.

# License

MIT
