# x2kafka

`x2kafka` is a high-performance, multi-protocol collector service designed to convert data from UNIX domain sockets, named pipes (FIFOs), Syslog (UDP/TCP/UNIX), Direct SNMP Traps, and HTTP(S) endpoints (including `http2k` / `k2http`) into Apache Kafka topics.

It is general, fully configurable via JSON, and compatible with `snmptrapd`, direct SNMP trap reception, `syslogd` / `rsyslog`, local UNIX domain sockets, REST APIs, or any custom tool exporting telemetry data.

---

## Key Features

- **Multi-Protocol Syslog Listener (`proto: "syslog"`, `"syslog_udp"`, `"syslog_tcp"`, `"syslog_unix"`)**: Parses RFC 3164 (BSD syslog) and RFC 5424 (IETF syslog) messages matching rsyslog schema fields. Specifying `"proto": "syslog"` automatically binds both UDP and TCP on the designated port.
- **HTTP / HTTPS & `http2k` / `k2http` Collector**:
  - **Transparent HTTP Decompression**: Automatically decompresses incoming GZIP, ZLIB, and raw DEFLATE payloads (supporting `k2http` compressed batches).
  - **Multi-Format Bulk Support**: Handles single JSON records, line-delimited records (NDJSON), JSON arrays (`[...]`), and concatenated JSON objects (`{...}{...}`).
  - **REST Path Routing & Sensor Extraction**: Parses REST URIs like `POST /rbdata/<sensor_uuid>/<topic>` (e.g., `POST /rbdata/bd2fe2c3-d0ff-4c27-aef9-122b698239b9/rb_flow`), extracting `sensor_uuid`, `proxy_uuid`, and the target Kafka topic dynamically.
- **UNIX Domain Sockets**: Supports both `unix` (Stream, SOCK_STREAM) and `unixgram` (Datagram, SOCK_DGRAM) sockets.
- **FIFO (Named Pipes)**: Continuously reads from Linux FIFOs with auto-reconnect handling for external writer processes.
- **Direct SNMP Trap Receiver**: Supports direct SNMP v1/v2c/v3 trap reception on UDP 162 (`snmptrap_udp`) with automatic numeric OID to MIB name translation using dynamic MIB directories (`mib_dirs`).
- **IP Security & Subnet Filtering**: Granular IP allowlisting (`allowed_ips`) and blacklisting (`blacklist_ips`) supporting single IPs and CIDR subnet blocks globally or per listener.
- **Message Splitting & Field Extraction**: Extract structured data from raw log strings using Key-Value parsing (`kv_extract`), CEF security headers (`cef_extract`), or custom named Regex patterns (`regex_patterns`).
- **Schema Standardization**: Map vendor-specific log keys (e.g., `src`, `srcip`, `source_ip`) to unified redBorder schema keys (`src_ip`, `dst_ip`, `src_port`, `dst_port`, `transport`, `username`) automatically via `schema_standardization`.
- **Per-Listener & Per-Rule Topic Routing**: Route incoming messages to dedicated Kafka topics per listener, per REST path, or per matched rule.
- **Dynamic & Static JSON Enrichment**: Flexible per-sensor lookup maps (`enrichment_map`) and conditional rules engine (`contains`, `match_key`) for tagging JSON payloads across all protocols.
- **Unix Timestamp Standard**: Output records contain `timestamp` set to standard Unix timestamp seconds (int64), identical to `redborder-monitor`.
- **High Throughput Scaling**: Parallel socket reader workers, configurable CPU worker pool, 8MB OS socket receive buffers, and LZ4 compression.
- **Graceful Shutdown & Hot Reload**: Supports `SIGHUP` signal to reload configuration without stopping the daemon, and clean channel draining during shutdown.
- **Telemetry & Debugging**: Built-in HTTP `/stats` endpoint for monitoring throughput and memory usage, with verbose debug mode (`-d` / `"debug": true`).

---

## IP Security & Access Control (`allowed_ips` & `blacklist_ips`)

`x2kafka` supports granular IP address and CIDR subnet filtering to restrict data ingestion to authorized senders:

```json
{
  "brokers": ["127.0.0.1:9092"],
  "topic": "rb_trap",
  "allowed_ips": ["10.1.33.0/24", "192.168.10.1"],
  "blacklist_ips": ["10.1.33.100"],
  "listeners": [
    {
      "name": "snmptrap-listener",
      "proto": "snmptrap_udp",
      "port": 162,
      "allowed_ips": ["192.168.10.0/24"]
    }
  ]
}
```

- **Allowlisting (`allowed_ips`)**: Accepts single IPs (`"192.168.10.1"`) or CIDR subnets (`"10.1.33.0/24"`). If specified, any sender IP outside these ranges is immediately dropped.
- **Blacklisting (`blacklist_ips`)**: Explicitly blocks designated IPs or subnets (e.g., `"192.168.101.3"`).
- Can be defined **globally** in the root configuration or **per-listener** (e.g. restricting SNMP traps to network management subnets).

---

## Message Splitting, Field Extraction & Schema Standardization

`x2kafka` supports extracting key-value fields from unstructured syslog text and mapping vendor-specific keys to a normalized schema:

```json
{
  "listeners": [
    {
      "name": "firewall-syslog-collector",
      "proto": "syslog",
      "bind": "0.0.0.0",
      "port": 514,
      "mode": "syslog",
      "kv_extract": true,
      "cef_extract": true,
      "schema_standardization": true,
      "regex_patterns": [
        "user=(?P<custom_user>\\S+)"
      ]
    }
  ]
}
```

- **`kv_extract: true`**: Automatically parses `key=value` or `key="value"` pairs (e.g. `srcip=192.168.1.50 dstip=10.0.0.1 action=deny`).
- **`cef_extract: true`**: Parses Common Event Format security headers (`CEF:0|Vendor|Product|...|src=10.0.0.1 dst=10.0.0.2`).
- **`regex_patterns`**: Uses named Go regular expression capture groups to extract custom payload fields (`(?P<fieldname>pattern)`).
- **`schema_standardization: true`**: Maps raw vendor keys into unified canonical schema fields:
  - `src`, `srcip`, `source_ip` $\rightarrow$ **`src_ip`**
  - `dst`, `dstip`, `destination_ip` $\rightarrow$ **`dst_ip`**
  - `spt`, `srcport` $\rightarrow$ **`src_port`**
  - `dpt`, `dstport` $\rightarrow$ **`dst_port`**
  - `proto`, `protocol` $\rightarrow$ **`transport`**
  - `user`, `usr`, `suser` $\rightarrow$ **`username`**

---

## SNMP MIB Auto-Discovery & Custom MIB Directories (`mib_dirs`)

`x2kafka` includes a built-in MIB engine for translating numeric SNMP trap OIDs (e.g., `.1.3.6.1.4.1.9.9.43.1.1.1.0`) into human-readable MIB variable names.

You can configure custom MIB directory locations in `config.json` via `"mib_dirs"`, matching redBorder platform standards:

```json
{
  "brokers": ["kafka.service:9092"],
  "topic": "rb_trap",
  "mib_dirs": [
    "/usr/share/snmp/mibs",
    "/var/lib/snmp/mibs",
    "/etc/redborder/mibs"
  ],
  "listeners": [
    {
      "name": "snmptrap-listener",
      "proto": "snmptrap",
      "bind": "0.0.0.0",
      "port": 162,
      "topic": "rb_trap"
    }
  ]
}
```

- **`mib_dirs`**: Array of directory paths containing vendor `.mib` or `.my` files.
- **System Fallback**: If `mib_dirs` is omitted, `x2kafka` automatically scans default Linux system locations (`/usr/share/snmp/mibs`).
- **Dynamic Resolution**: As traps arrive over UDP/TCP port 162, numeric OIDs are automatically replaced with MIB names and flattened into the output JSON.

---

## Complete Example Configuration (`config.syslog.json`)

Below is a complete, production-ready configuration combining Syslog (UDP & TCP), `http2k` REST endpoint, per-sensor dynamic enrichment, and schema standardization:

```json
{
  "debug": false,
  "brokers": ["kafka.service:9092"],
  "topic": "x2kafka_default",
  "num_workers": 16,
  "channel_buffer_size": 50000,
  "socket_buffer_bytes": 8388608,
  "listeners": [
    {
      "name": "syslog-collector",
      "proto": "syslog",
      "bind": "0.0.0.0",
      "port": 514,
      "mode": "syslog",
      "kv_extract": true,
      "cef_extract": true,
      "schema_standardization": true,
      "enrichment_rules": [
        {
          "match_key": "fromhost_ip",
          "match_value": "10.1.33.158",
          "topic": "rb_vault",
          "enrichment": {
            "sensor_ip": "10.1.33.158",
            "sensor_name": "vaultD",
            "sensor_uuid": "e90902da-cec3-4020-9eaf-708ee32f86ac"
          }
        },
        {
          "match_fields": {
            "fromhost_ip": "127.0.0.1",
            "app_name": "cep"
          },
          "contains": ["ALERT", "RULE"],
          "topic": "rb_vault",
          "enrichment": {
            "sensor_ip": "127.0.0.1",
            "sensor_name": "CEP",
            "sensor_uuid": "f6eb42c9-7003-49f3-9a3b-4da6211d5c37"
          }
        }
      ]
    },
    {
      "name": "http2k-listener",
      "proto": "http",
      "bind": "0.0.0.0",
      "port": 7980,
      "mode": "json",
      "enrichment_key": "sensor_uuid",
      "enrichment_map": {
        "bd2fe2c3-d0ff-4c27-aef9-122b698239b9": {
          "index_partitions": 5,
          "index_replicas": 1,
          "proxy_uuid": "bd2fe2c3-d0ff-4c27-aef9-122b698239b9",
          "sensor_uuid": "bd2fe2c3-d0ff-4c27-aef9-122b698239b9"
        }
      }
    },
    {
      "name": "snmptrap-listener",
      "proto": "snmptrap",
      "bind": "0.0.0.0",
      "port": 162,
      "topic": "rb_trap",
      "allowed_ips": ["10.1.33.158", "127.0.0.1"],
      "enrichment_rules": [
        {
          "match_key": "source_addr",
          "match_value": "10.1.33.158",
          "topic": "rb_trap",
          "enrichment": {
            "sensor_ip": "10.1.33.158",
            "sensor_name": "trapD",
            "sensor_uuid": "e90902da-cec3-4020-9eaf-708ee32f86ac"
          }
        }
      ]
    }
  ]
}
```

---

## Estimated Performance & Throughput Benchmarks

Tested on a standard 8-core CPU server with 16 GB RAM and LZ4 compression enabled:

| Input Protocol | Estimated Throughput | Memory (RAM) Usage | Key Performance Factor |
| :--- | :--- | :--- | :--- |
| **Direct UDP Syslog (`syslog_udp`)** | **80,000 – 120,000 events / sec** | ~35 MB – 65 MB | 8 MB Kernel socket receive buffer (`SO_RCVBUF`) + Parallel socket workers. |
| **Direct SNMP Traps (`snmptrap_udp`)** | **50,000 – 90,000 traps / sec** | ~40 MB – 70 MB | Concurrent Varbind decoding & in-memory `gomib` OID resolution. |
| **HTTP Bulk Array / Deflate (`http` POST)** | **150,000 – 250,000 events / sec** | ~30 MB – 60 MB | Transparent `GZIP`/`Deflate` streaming + Concatenated JSON tokenizer. |
| **UNIX Socket / FIFO (`unix` / `fifo`)** | **100,000 – 180,000 events / sec** | ~25 MB – 50 MB | Zero network stack overhead; in-memory `memcpy` performance. |

---

## CLI Usage & Live Statistics

`x2kafka` provides command-line flags to inspect live statistics, run in debug mode, or reload configuration:

```bash
# Fetch and print live JSON statistics from the running daemon:
x2kafka -stats -c /etc/x2kafka/config.json

# Run daemon in verbose debug mode:
x2kafka -d -c /etc/x2kafka/config.json

# Show version:
x2kafka -v
```

### Fetching Live Stats via UNIX Socket, HTTP REST API, or CLI:

`x2kafka` supports querying status via UNIX domain socket (IPC) matching `redborder-monitor`, as well as via HTTP REST API:

```bash
# Query via CLI flag (auto-detects socket or HTTP stats port):
x2kafka -stats -c /etc/x2kafka/config.json

# Query via UNIX Domain Socket (redborder-monitor IPC protocol):
echo "stats" | nc -U /run/x2kafka/x2kafka.sock

# Query via UNIX Socket HTTP GET:
curl --unix-socket /run/x2kafka/x2kafka.sock http://localhost/stats

# Query via HTTP REST API port:
curl http://127.0.0.1:7979/stats
```

Example JSON response:
```json
{
  "uptime_seconds": 1420,
  "messages_received": 542910,
  "messages_sent_kafka": 542910,
  "messages_failed_kafka": 0,
  "bytes_received": 148291024,
  "bytes_sent_kafka": 159201948,
  "alloc_bytes": 14920184,
  "sys_bytes": 35201928,
  "num_gc": 42,
  "listeners": [
    {
      "name": "syslog-collector-udp",
      "proto": "syslog_udp",
      "messages_received": 210000,
      "bytes_received": 42000000,
      "last_message_timestamp_ms": 1790619568123,
      "error_count": 0
    },
    {
      "name": "snmptrap-listener-udp",
      "proto": "snmptrap_udp",
      "messages_received": 4500,
      "bytes_received": 950000,
      "last_message_timestamp_ms": 1790619568150,
      "error_count": 0
    }
  ]
}
```

---

## Installation & Build

```bash
make build
```

The resulting binary will be placed in `bin/x2kafka`.

To run tests:
```bash
make test
```

To install:
```bash
sudo make install
```

---

## Service Management

A systemd unit file is provided under `resources/systemd/x2kafka.service`.

```bash
sudo cp resources/systemd/x2kafka.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now x2kafka
```
