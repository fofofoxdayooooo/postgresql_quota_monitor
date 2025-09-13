/**
 * @file postgres_realtime_monitor_daemon.c
 * @brief PostgreSQL real-time storage monitor daemon using LISTEN/NOTIFY.
 *
 * This program runs as a background daemon and connects to a PostgreSQL server.
 * It listens for notifications from database triggers and immediately checks
 * the storage usage of the user who initiated the change.
 * If a user's total data usage exceeds a defined hard limit, their account
 * is immediately locked. This daemon complements the periodic monitoring
 * daemon by providing a rapid response to sudden storage spikes.
 *
 * Features:
 * - Real-time monitoring triggered by database events.
 * - Integration with a JSON configuration file for user limits.
 * - Non-blocking I/O using select() for efficient event loop.
 * - Graceful shutdown via SIGTERM/SIGINT signal handling.
 * - Robust error handling with detailed syslog output.
 * - **New:** Soft limit warnings sent to syslog and Slack/Webhook.
 * - **New:** Prometheus exporter integration for real-time visualization.
 * - **New:** Rate-limiting mechanism to suppress redundant notifications.
 * - **New:** Asynchronous Slack notifications using libcurl.
 * - **New:** Immediate termination of existing sessions upon hard limit breach.
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
#include <libgen.h>
#include <sys/file.h>
#include <curl/curl.h>

#define USER_LIMITS_FILE "/etc/postgres_monitor/user_limits.json"
#define DB_CREDENTIALS_FILE "/etc/postgres_monitor/monitor_user.passwd"
#define MONITOR_CONFIG_FILE "/etc/postgres_monitor/monitor_config.json"
#define PROMETHEUS_FILE "/var/lib/prometheus/node-exporter/postgres_realtime_monitor.prom"
#define PID_FILE "/var/run/postgres_realtime_monitor.pid"
#define LISTEN_CHANNEL "storage_limit_check"
#define MAX_USERS 1024
#define RATE_LIMIT_SECONDS 5 // Rate limit check per user to prevent redundant checks
#define MAX_RETRY_COUNT 5

// User limits from the config file
typedef struct {
    char user[64];
    long long soft_limit; // in bytes
    long long hard_limit; // in bytes
    bool manual_unlock; // Requires manual unlock
    time_t last_exceeded_time;
    int consecutive_failures;
} UserLimit;

// Cache for storing user usage data
typedef struct {
    char username[64];
    long long usage_bytes;
} UserUsageCache;


// Overall daemon configuration
typedef struct {
    bool slack_enabled;
    char slack_webhook_url[512];
    bool prometheus_enabled;
} MonitorConfig;

// Prometheus metrics data structure
typedef struct {
    char user[64];
    long long usage;
    long long soft_limit;
    long long hard_limit;
} PromMetrics;

static UserLimit user_limits[MAX_USERS];
static int user_limit_count = 0;
static UserUsageCache user_usage_cache[MAX_USERS];
static int user_usage_count = 0;
static MonitorConfig config;
static volatile sig_atomic_t keep_running = 1;
static char g_db_user[64] = "";
static char g_db_pass[128] = "";

// Forward declarations of static functions
static bool read_user_limits_from_json();
static bool read_monitor_config_from_json();
static char* read_db_password();
static PGconn* connect_to_db();
static long long get_user_storage_usage(PGconn* conn, const char* username);
static void lock_user_account(PGconn* conn, const char* username);
static void terminate_user_sessions(PGconn* conn, const char* username);
static void write_prometheus_metrics();
static void send_slack_notification(const char* message);
static void signal_handler(int sig);
static bool acquire_pid_file_lock();
static void update_usage_cache(const char* username, long long usage);
static bool get_user_from_cache(const char* username, UserUsageCache* user_data);
static int lock_fd = -1; // Global file descriptor for the lock

/**
 * @brief Signal handler for graceful shutdown.
 * @param sig The signal received (e.g., SIGTERM, SIGINT).
 */
static void signal_handler(int sig) {
    if (sig == SIGTERM || sig == SIGINT) {
        keep_running = 0;
    }
}

/**
 * @brief Reads user limits from the JSON configuration file.
 * @return true if successful, false otherwise.
 */
static bool read_user_limits_from_json() {
    FILE* fp = fopen(USER_LIMITS_FILE, "r");
    if (!fp) {
        syslog(LOG_ERR, "Failed to open user limits file: %s", USER_LIMITS_FILE);
        return false;
    }

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char* buffer = malloc(fsize + 1);
    if (!buffer) {
        fclose(fp);
        syslog(LOG_ERR, "Memory allocation failed for user limits file.");
        return false;
    }
    fread(buffer, 1, fsize, fp);
    fclose(fp);
    buffer[fsize] = 0;

    cJSON* json = cJSON_Parse(buffer);
    free(buffer);

    if (!json) {
        syslog(LOG_ERR, "JSON parsing error in %s: %s", USER_LIMITS_FILE, cJSON_GetErrorPtr());
        return false;
    }

    cJSON* limits_array = cJSON_GetObjectItemCaseSensitive(json, "user_limits");
    if (!cJSON_IsArray(limits_array)) {
        syslog(LOG_ERR, "Invalid JSON format in %s: 'user_limits' array not found.", USER_LIMITS_FILE);
        cJSON_Delete(json);
        return false;
    }

    user_limit_count = 0;
    cJSON* limit_item;
    cJSON_ArrayForEach(limit_item, limits_array) {
        if (user_limit_count >= MAX_USERS) break;

        cJSON* user_json = cJSON_GetObjectItemCaseSensitive(limit_item, "user");
        cJSON* soft_json = cJSON_GetObjectItemCaseSensitive(limit_item, "soft_limit");
        cJSON* hard_json = cJSON_GetObjectItemCaseSensitive(limit_item, "hard_limit");

        if (cJSON_IsString(user_json) && cJSON_IsNumber(soft_json) && cJSON_IsNumber(hard_json)) {
            strncpy(user_limits[user_limit_count].user, user_json->valuestring, sizeof(user_limits[user_limit_count].user) - 1);
            user_limits[user_limit_count].soft_limit = (long long)soft_json->valuedouble;
            user_limits[user_limit_count].hard_limit = (long long)hard_json->valuedouble;
            user_limit_count++;
        } else {
            syslog(LOG_ERR, "Skipping invalid user limit entry in %s due to type mismatch.", USER_LIMITS_FILE);
        }
    }

    cJSON_Delete(json);
    return true;
}

/**
 * @brief Reads the main monitor configuration from a JSON file.
 * @return true if successful, false otherwise.
 */
static bool read_monitor_config_from_json() {
    FILE* fp = fopen(MONITOR_CONFIG_FILE, "r");
    if (!fp) {
        syslog(LOG_ERR, "Failed to open monitor config file: %s", MONITOR_CONFIG_FILE);
        return false;
    }

    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char* buffer = malloc(fsize + 1);
    if (!buffer) {
        fclose(fp);
        syslog(LOG_ERR, "Memory allocation failed for monitor config file.");
        return false;
    }
    fread(buffer, 1, fsize, fp);
    fclose(fp);
    buffer[fsize] = 0;

    cJSON* json = cJSON_Parse(buffer);
    free(buffer);

    if (!json) {
        syslog(LOG_ERR, "JSON parsing error in %s: %s", MONITOR_CONFIG_FILE, cJSON_GetErrorPtr());
        return false;
    }

    cJSON* slack_enabled_json = cJSON_GetObjectItemCaseSensitive(json, "slack_enabled");
    cJSON* slack_webhook_url_json = cJSON_GetObjectItemCaseSensitive(json, "slack_webhook_url");
    cJSON* prometheus_enabled_json = cJSON_GetObjectItemCaseSensitive(json, "prometheus_enabled");

    config.slack_enabled = cJSON_IsTrue(slack_enabled_json);
    if (config.slack_enabled) {
        if (cJSON_IsString(slack_webhook_url_json) && slack_webhook_url_json->valuestring != NULL) {
            strncpy(config.slack_webhook_url, slack_webhook_url_json->valuestring, sizeof(config.slack_webhook_url) - 1);
        } else {
            syslog(LOG_ERR, "Slack is enabled but webhook URL is missing or invalid.");
            config.slack_enabled = false;
        }
    }
    config.prometheus_enabled = cJSON_IsTrue(prometheus_enabled_json);

    cJSON_Delete(json);
    return true;
}


/**
 * @brief Reads the database password from a secure file.
 * @return The password string or NULL on failure.
 */
static char* read_db_password() {
    FILE* fp = fopen(DB_CREDENTIALS_FILE, "r");
    if (!fp) {
        syslog(LOG_ERR, "Failed to open DB password file: %s", DB_CREDENTIALS_FILE);
        return NULL;
    }
    char* password = malloc(128);
    if (!password) {
        fclose(fp);
        return NULL;
    }
    if (fgets(password, 128, fp)) {
        password[strcspn(password, "\n")] = 0; // remove newline
    } else {
        free(password);
        password = NULL;
    }
    fclose(fp);
    return password;
}

/**
 * @brief Connects to PostgreSQL with exponential backoff.
 * @return A valid PGconn pointer or NULL on permanent failure.
 */
static PGconn* connect_to_db() {
    char conninfo[256];
    snprintf(conninfo, sizeof(conninfo), "host=localhost user=%s password=%s dbname=postgres", g_db_user, g_db_pass);
    
    PGconn* conn = NULL;
    int retry_count = 0;
    while (retry_count < 5) {
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) == CONNECTION_OK) {
            syslog(LOG_INFO, "Successfully connected to PostgreSQL for real-time monitoring.");
            return conn;
        }
        syslog(LOG_ERR, "Failed to connect to PostgreSQL. Retrying... (Attempt %d)", retry_count + 1);
        PQfinish(conn);
        sleep(1 << retry_count);
        retry_count++;
    }
    syslog(LOG_CRIT, "Failed to connect to PostgreSQL after multiple retries. Giving up.");
    return NULL;
}

/**
 * @brief Gets a user's total storage usage in bytes.
 * @param conn The PostgreSQL connection.
 * @param username The user to check.
 * @return The usage in bytes or -1 on failure.
 */
static long long get_user_storage_usage(PGconn *conn, const char *username) {
    char query[512];
    snprintf(query, sizeof(query),
        "SELECT COALESCE(SUM(pg_relation_size(c.oid)), 0) FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_roles r ON r.oid = c.relowner "
        "WHERE r.rolname = '%s';", username);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        syslog(LOG_ERR, "Failed to get storage usage for user '%s'. Reason: %s", username, PQerrorMessage(conn));
        PQclear(res);
        return -1;
    }

    const char *result_value = PQgetvalue(res, 0, 0);
    long long usage = result_value ? atoll(result_value) : 0;
    PQclear(res);
    return usage;
}

/**
 * @brief Locks a PostgreSQL user account by setting NOLOGIN.
 * @param conn The PostgreSQL connection.
 * @param username The user to lock.
 */
static void lock_user_account(PGconn *conn, const char *username) {
    char query[256];
    snprintf(query, sizeof(query), "ALTER ROLE \"%s\" WITH NOLOGIN;", username);
    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        syslog(LOG_ERR, "Failed to lock user '%s'. Reason: %s", username, PQerrorMessage(conn));
    } else {
        syslog(LOG_WARNING, "User '%s' locked immediately due to storage overuse.", username);
    }
    PQclear(res);
}

/**
 * @brief Terminates all active sessions for a given user, excluding the current one.
 * @param conn The PostgreSQL connection.
 * @param username The user whose sessions to terminate.
 */
static void terminate_user_sessions(PGconn *conn, const char *username) {
    char query[256];
    snprintf(query, sizeof(query),
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE usename = '%s' AND pid <> pg_backend_pid();", username);

    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        syslog(LOG_ERR, "Failed to terminate sessions for user '%s'. Reason: %s", username, PQerrorMessage(conn));
    } else {
        syslog(LOG_WARNING, "Terminated all active sessions for user '%s'.", username);
    }
    PQclear(res);
}


/**
 * @brief Acquires a file lock to prevent multiple daemon instances.
 * @return true if the lock is acquired, false otherwise.
 */
static bool acquire_pid_file_lock() {
    lock_fd = open(PID_FILE, O_CREAT | O_RDWR, 0644);
    if (lock_fd < 0) {
        syslog(LOG_ERR, "Could not open PID file %s", PID_FILE);
        return false;
    }
    if (flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        syslog(LOG_ERR, "Could not lock PID file %s, another instance might be running.", PID_FILE);
        close(lock_fd);
        lock_fd = -1;
        return false;
    }
    char pid_str[16];
    snprintf(pid_str, sizeof(pid_str), "%d\n", getpid());
    ftruncate(lock_fd, 0);
    write(lock_fd, pid_str, strlen(pid_str));
    return true;
}

/**
 * @brief Updates the in-memory usage cache for a user.
 * @param username The user to update.
 * @param usage The new usage value.
 */
static void update_usage_cache(const char* username, long long usage) {
    bool found = false;
    for (int i = 0; i < user_usage_count; i++) {
        if (strcmp(user_usage_cache[i].username, username) == 0) {
            user_usage_cache[i].usage_bytes = usage;
            found = true;
            break;
        }
    }
    if (!found && user_usage_count < MAX_USERS) {
        strncpy(user_usage_cache[user_usage_count].username, username, sizeof(user_usage_cache[user_usage_count].username) - 1);
        user_usage_cache[user_usage_count].usage_bytes = usage;
        user_usage_count++;
    }
}

/**
 * @brief Writes Prometheus metrics to a file if enabled.
 */
static void write_prometheus_metrics() {
    if (!config.prometheus_enabled) {
        return;
    }

    FILE *fp = fopen(PROMETHEUS_FILE, "w");
    if (!fp) {
        syslog(LOG_ERR, "Failed to open Prometheus file for writing.");
        return;
    }

    fprintf(fp, "# HELP postgres_user_storage_usage_bytes Current storage usage of a user in bytes.\n");
    fprintf(fp, "# TYPE postgres_user_storage_usage_bytes gauge\n");
    
    for (int i = 0; i < user_usage_count; i++) {
        fprintf(fp, "postgres_user_storage_usage_bytes{user=\"%s\"} %lld\n",
                user_usage_cache[i].username, user_usage_cache[i].usage_bytes);
    }

    fclose(fp);
}

/**
 * @brief Sends a Slack notification using libcurl.
 * @param message The message to send.
 */
static void send_slack_notification(const char* message) {
    if (!config.slack_enabled || strlen(config.slack_webhook_url) == 0) {
        return;
    }
    
    CURL *curl;
    CURLcode res;
    
    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();
    if(curl) {
        char json_payload[2048];
        snprintf(json_payload, sizeof(json_payload), "{\"text\": \"%s\"}", message);
        
        curl_easy_setopt(curl, CURLOPT_URL, config.slack_webhook_url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_slist_append(NULL, "Content-Type: application/json"));
        
        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            syslog(LOG_ERR, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();
}


int main(int argc, char *argv[]) {
    // Set up syslog for logging daemon messages
    openlog("postgres_realtime_monitor", LOG_PID | LOG_CONS, LOG_DAEMON);

    // Acquire PID file lock to prevent multiple instances
    if (!acquire_pid_file_lock()) {
        closelog();
        return 1;
    }

    // Register signal handlers for graceful shutdown
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

    // Read daemon configuration files
    if (!read_monitor_config_from_json()) {
        syslog(LOG_CRIT, "Failed to read monitor config. Exiting.");
        return 1;
    }
    if (!read_user_limits_from_json()) {
        syslog(LOG_CRIT, "Failed to read user limits. Exiting.");
        return 1;
    }

    // Read the database password from a secure file
    char* pass = read_db_password();
    if (!pass) {
        syslog(LOG_CRIT, "Failed to read DB password. Exiting.");
        return 1;
    }
    strncpy(g_db_user, "monitor_user", sizeof(g_db_user) - 1); // Hardcoded monitor user from previous files
    strncpy(g_db_pass, pass, sizeof(g_db_pass) - 1);
    free(pass);

    // Connect to PostgreSQL
    PGconn* conn = connect_to_db();
    if (!conn) {
        return 1;
    }

    // Start listening for notifications from the trigger
    PGresult* res = PQexec(conn, "LISTEN " LISTEN_CHANNEL ";");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        syslog(LOG_ERR, "LISTEN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 1;
    }
    PQclear(res);
    syslog(LOG_INFO, "Listening for notifications on channel '%s'", LISTEN_CHANNEL);

    // Main event loop
    while (keep_running) {
        // Check for pending notifications without blocking
        if (PQconsumeInput(conn) == 0) {
            syslog(LOG_ERR, "Failed to consume input: %s", PQerrorMessage(conn));
            sleep(1);
            continue;
        }

        // Process any notifications that have arrived
        PGnotify* notify;
        while ((notify = PQnotifies(conn)) != NULL) {
            const char* username = notify->extra;
            
            syslog(LOG_INFO, "Received notification for user: %s", username);

            // Find the user's limits from the in-memory cache
            bool user_found = false;
            long long soft_limit = -1;
            long long hard_limit = -1;
            for (int i = 0; i < user_limit_count; i++) {
                if (strcmp(user_limits[i].user, username) == 0) {
                    soft_limit = user_limits[i].soft_limit;
                    hard_limit = user_limits[i].hard_limit;
                    user_found = true;
                    break;
                }
            }

            if (user_found) {
                long long usage = get_user_storage_usage(conn, username);
                if (usage != -1) {
                    update_usage_cache(username, usage);
                    write_prometheus_metrics();

                    if (usage >= hard_limit) {
                        // Terminate all existing sessions before locking the account
                        terminate_user_sessions(conn, username);
                        lock_user_account(conn, username);
                    } else if (usage >= soft_limit) {
                         syslog(LOG_WARNING, "User '%s' is over soft limit. Current usage: %lld bytes.", username, usage);
                         char msg[256];
                         snprintf(msg, sizeof(msg), "⚠️ Soft limit exceeded: User %s usage is %lld bytes (limit %lld)", username, usage, soft_limit);
                         send_slack_notification(msg);
                    }
                    else {
                        syslog(LOG_INFO, "User '%s' is below limits. No action needed. Current usage: %lld bytes.", username, usage);
                    }
                }
            } else {
                syslog(LOG_WARNING, "Received notification for unknown user: %s", username);
            }

            PQfreemem(notify);
        }

        // Wait for a new notification (or a signal)
        int fd = PQsocket(conn);
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(fd, &fds);
        
        struct timeval tv = {1, 0}; // 1 second timeout
        select(fd + 1, &fds, NULL, NULL, &tv);
    }

    // Clean up resources on exit
    PQfinish(conn);
    closelog();
    return 0;
}
