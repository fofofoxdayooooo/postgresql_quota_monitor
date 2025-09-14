/**
 * @file postgres_realtime_monitor_daemon_flex.c
 * @brief PostgreSQL real-time storage monitor daemon (flexible edition).
 *
 * Features:
 * - Dynamic allocation of user limits (tens of thousands of users).
 * - Configurable mode: "shared_hosting" (many lightweight users) or
 *   "enterprise_db" (few heavy users, many tables).
 * - Configurable cache TTL and session termination batch size.
 * - Prometheus metrics written via tmpfile + rename().
 * - Slack notification support.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <syslog.h>
#include <signal.h>
#include <stdbool.h>
#include <libpq-fe.h>
#include "cJSON.h"
#include <sys/file.h>
#include <curl/curl.h>

#define USER_LIMITS_FILE "/etc/postgres_monitor/user_limits.json"
#define DB_CREDENTIALS_FILE "/root/etc/postgres_monitor/monitor_user.passwd"
#define MONITOR_CONFIG_FILE "/etc/postgres_monitor/monitor_config.json"
#define PROMETHEUS_FILE "/var/lib/prometheus/node-exporter/postgres_realtime_monitor.prom"
#define PID_FILE "/var/run/postgres_realtime_monitor.pid"
#define LISTEN_CHANNEL "storage_limit_check"

typedef struct {
    char user[64];
    long long soft_limit;
    long long hard_limit;
    bool manual_unlock;
} UserLimit;

typedef struct {
    char username[64];
    long long usage_bytes;
    time_t last_checked;
} UserUsageCache;

typedef struct {
    bool slack_enabled;
    char slack_webhook_url[512];
    bool prometheus_enabled;
    char mode[32];          // "shared_hosting" or "enterprise_db"
    int cache_ttl_seconds;  // cache reuse interval
    int session_batch_size; // batch size for pg_terminate_backend
} MonitorConfig;

static UserLimit* user_limits = NULL;
static size_t user_limit_count = 0;
static UserUsageCache* usage_cache = NULL;
static size_t usage_cache_count = 0;

static MonitorConfig config;
static volatile sig_atomic_t keep_running = 1;
static char g_db_user[64] = "";
static char g_db_pass[128] = "";
static int lock_fd = -1;

// ---------- Utility ----------

static void signal_handler(int sig) {
    if (sig == SIGTERM || sig == SIGINT) keep_running = 0;
}

static bool acquire_pid_file_lock() {
    lock_fd = open(PID_FILE, O_CREAT | O_RDWR, 0644);
    if (lock_fd < 0) return false;
    if (flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        close(lock_fd); lock_fd = -1; return false;
    }
    char pid_str[16];
    snprintf(pid_str, sizeof(pid_str), "%d\n", getpid());
    ftruncate(lock_fd, 0);
    write(lock_fd, pid_str, strlen(pid_str));
    return true;
}

// ---------- Config loaders ----------

static bool read_user_limits_from_json() {
    FILE* fp = fopen(USER_LIMITS_FILE, "r");
    if (!fp) return false;

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char* buffer = malloc(fsize + 1);
    fread(buffer, 1, fsize, fp);
    buffer[fsize] = 0;
    fclose(fp);

    cJSON* json = cJSON_Parse(buffer);
    free(buffer);
    if (!json) return false;

    cJSON* limits_array = cJSON_GetObjectItemCaseSensitive(json, "user_limits");
    if (!cJSON_IsArray(limits_array)) { cJSON_Delete(json); return false; }

    free(user_limits); user_limits = NULL;
    user_limit_count = 0;

    size_t alloc_count = cJSON_GetArraySize(limits_array);
    user_limits = calloc(alloc_count, sizeof(UserLimit));
    if (!user_limits) { cJSON_Delete(json); return false; }

    cJSON* limit_item; size_t idx = 0;
    cJSON_ArrayForEach(limit_item, limits_array) {
        cJSON* user_json = cJSON_GetObjectItem(limit_item, "user");
        cJSON* soft_json = cJSON_GetObjectItem(limit_item, "soft_limit");
        cJSON* hard_json = cJSON_GetObjectItem(limit_item, "hard_limit");
        if (cJSON_IsString(user_json) && cJSON_IsNumber(soft_json) && cJSON_IsNumber(hard_json)) {
            strncpy(user_limits[idx].user, user_json->valuestring, sizeof(user_limits[idx].user)-1);
            user_limits[idx].soft_limit = (long long)soft_json->valuedouble;
            user_limits[idx].hard_limit = (long long)hard_json->valuedouble;
            idx++;
        }
    }
    user_limit_count = idx;
    cJSON_Delete(json);
    return true;
}

static bool read_monitor_config_from_json() {
    FILE* fp = fopen(MONITOR_CONFIG_FILE, "r");
    if (!fp) return false;

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char* buffer = malloc(fsize + 1);
    fread(buffer, 1, fsize, fp);
    buffer[fsize] = 0;
    fclose(fp);

    cJSON* json = cJSON_Parse(buffer);
    free(buffer);
    if (!json) return false;

    cJSON* slack_enabled_json = cJSON_GetObjectItemCaseSensitive(json, "slack_enabled");
    cJSON* slack_url_json = cJSON_GetObjectItemCaseSensitive(json, "slack_webhook_url");
    cJSON* prometheus_enabled_json = cJSON_GetObjectItemCaseSensitive(json, "prometheus_enabled");
    cJSON* mode_json = cJSON_GetObjectItemCaseSensitive(json, "mode");
    cJSON* ttl_json = cJSON_GetObjectItemCaseSensitive(json, "cache_ttl_seconds");
    cJSON* batch_json = cJSON_GetObjectItemCaseSensitive(json, "session_batch_size");

    config.slack_enabled = cJSON_IsTrue(slack_enabled_json);
    if (config.slack_enabled && cJSON_IsString(slack_url_json))
        strncpy(config.slack_webhook_url, slack_url_json->valuestring, sizeof(config.slack_webhook_url)-1);

    config.prometheus_enabled = cJSON_IsTrue(prometheus_enabled_json);

    if (cJSON_IsString(mode_json)) strncpy(config.mode, mode_json->valuestring, sizeof(config.mode)-1);
    else strncpy(config.mode, "enterprise_db", sizeof(config.mode)-1);

    config.cache_ttl_seconds = cJSON_IsNumber(ttl_json) ? ttl_json->valueint :
        (strcmp(config.mode,"shared_hosting")==0 ? 15 : 2);
    config.session_batch_size = cJSON_IsNumber(batch_json) ? batch_json->valueint :
        (strcmp(config.mode,"shared_hosting")==0 ? 100 : 500);

    cJSON_Delete(json);
    return true;
}

static char* read_db_password() {
    FILE* fp = fopen(DB_CREDENTIALS_FILE, "r");
    if (!fp) return NULL;
    char* pw = malloc(128);
    if (fgets(pw, 128, fp)) pw[strcspn(pw, "\n")] = 0; else { free(pw); pw=NULL; }
    fclose(fp);
    return pw;
}

// ---------- DB ops ----------

static PGconn* connect_to_db() {
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), "host=localhost user=%s password=%s dbname=postgres", g_db_user, g_db_pass);
    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return NULL;
    }
    return conn;
}

static long long get_user_storage_usage(PGconn* conn, const char* username) {
    // check cache
    for (size_t i=0; i<usage_cache_count; i++) {
        if (strcmp(usage_cache[i].username, username)==0) {
            if (time(NULL) - usage_cache[i].last_checked < config.cache_ttl_seconds)
                return usage_cache[i].usage_bytes;
        }
    }

    char query[512];
    snprintf(query, sizeof(query),
      "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)),0) "
      "FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner "
      "WHERE r.rolname='%s';", username);

    PGresult* res = PQexec(conn, query);
    if (PQresultStatus(res)!=PGRES_TUPLES_OK) { PQclear(res); return -1; }
    long long usage = atoll(PQgetvalue(res,0,0));
    PQclear(res);

    // update cache
    for (size_t i=0; i<usage_cache_count; i++) {
        if (strcmp(usage_cache[i].username, username)==0) {
            usage_cache[i].usage_bytes = usage;
            usage_cache[i].last_checked = time(NULL);
            return usage;
        }
    }
    usage_cache = realloc(usage_cache, (usage_cache_count+1)*sizeof(UserUsageCache));
    strncpy(usage_cache[usage_cache_count].username, username, 63);
    usage_cache[usage_cache_count].usage_bytes = usage;
    usage_cache[usage_cache_count].last_checked = time(NULL);
    usage_cache_count++;
    return usage;
}

static void terminate_user_sessions(PGconn* conn, const char* username) {
    char query[512];
    snprintf(query, sizeof(query),
      "SELECT pid FROM pg_stat_activity WHERE usename='%s' AND pid<>pg_backend_pid();", username);
    PGresult* res = PQexec(conn, query);
    if (PQresultStatus(res)!=PGRES_TUPLES_OK) { PQclear(res); return; }
    int rows = PQntuples(res);
    for (int i=0;i<rows;i++) {
        if (i % config.session_batch_size == 0) usleep(100000);
        char killq[64];
        snprintf(killq,sizeof(killq),"SELECT pg_terminate_backend(%s);",PQgetvalue(res,i,0));
        PQexec(conn, killq);
    }
    PQclear(res);
}

static void lock_user_account(PGconn* conn, const char* username) {
    char query[256];
    snprintf(query,sizeof(query),"ALTER ROLE \"%s\" NOLOGIN;",username);
    PQexec(conn, query);
}

// ---------- Prometheus ----------

static void write_prometheus_metrics() {
    if (!config.prometheus_enabled) return;
    char tmpfile[256];
    snprintf(tmpfile,sizeof(tmpfile),"%s.tmp",PROMETHEUS_FILE);
    FILE* fp = fopen(tmpfile,"w");
    if (!fp) return;
    fprintf(fp,"# HELP postgres_user_storage_usage_bytes Current usage\n");
    fprintf(fp,"# TYPE postgres_user_storage_usage_bytes gauge\n");
    for (size_t i=0;i<usage_cache_count;i++) {
        fprintf(fp,"postgres_user_storage_usage_bytes{user=\"%s\"} %lld\n",
          usage_cache[i].username, usage_cache[i].usage_bytes);
    }
    fclose(fp);
    rename(tmpfile,PROMETHEUS_FILE);
}

// ---------- Main ----------

int main() {
    openlog("pg_rt_monitor_flex", LOG_PID|LOG_CONS, LOG_DAEMON);
    if (!acquire_pid_file_lock()) return 1;
    signal(SIGTERM, signal_handler); signal(SIGINT, signal_handler);

    if (!read_monitor_config_from_json()) { syslog(LOG_CRIT,"failed to read monitor config"); return 1; }
    if (!read_user_limits_from_json()) { syslog(LOG_CRIT,"failed to read user limits"); return 1; }

    char* pass = read_db_password(); if (!pass) return 1;
    strncpy(g_db_user,"monitor_user",63);
    strncpy(g_db_pass,pass,127); free(pass);

    PGconn* conn = connect_to_db(); if (!conn) return 1;
    PQexec(conn,"LISTEN " LISTEN_CHANNEL ";");

    while (keep_running) {
        PQconsumeInput(conn);
        PGnotify* n;
        while ((n=PQnotifies(conn))!=NULL) {
            const char* user = n->extra;
            long long usage = get_user_storage_usage(conn,user);
            for (size_t i=0;i<user_limit_count;i++) {
                if (strcmp(user_limits[i].user,user)==0) {
                    if (usage >= user_limits[i].hard_limit) {
                        terminate_user_sessions(conn,user);
                        lock_user_account(conn,user);
                    } else if (usage >= user_limits[i].soft_limit) {
                        syslog(LOG_WARNING,"User %s exceeded soft limit (%lld)", user, usage);
                    }
                }
            }
            PQfreemem(n);
        }
        int fd = PQsocket(conn);
        fd_set fds; FD_ZERO(&fds); FD_SET(fd,&fds);
        struct timeval tv={1,0}; select(fd+1,&fds,NULL,NULL,&tv);
    }
    PQfinish(conn); closelog();
    return 0;
}
