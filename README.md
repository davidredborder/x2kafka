# x2kafka & kafka2x

This repository provides two high-performance streaming daemons for the redBorder platform:

1. **`x2kafka`** (Ingress): Collects data events from UNIX domain sockets, named pipes (FIFOs), Syslog (UDP/TCP/UNIX), Direct SNMP Traps, and HTTP(S) endpoints into Apache Kafka topics.
2. **`kafka2x`** (Egress): Consumes events from Kafka topics and reliably forwards them in batches to remote HTTP(S) REST bulk endpoints with compression, framing, and automatic topic-qualified routing (replaces legacy `k2http` / `rbforwarder`).

---

## Key Features

### `x2kafka` (Ingress to Kafka)
- **Multi-Protocol Syslog Listener (`proto: "syslog"`, `"syslog_udp"`, `"syslog_tcp"`, `"syslog_unix"`)**: Parses RFC 3164 (BSD syslog) and RFC 5424 (IETF syslog) messages matching rsyslog schema fields. Specifying `"proto": "syslog"` automatically binds both UDP and TCP on the designated port.
- **HTTP / HTTPS & `http2k` Collector**:
  - **Transparent HTTP Decompression**: Automatically decompresses incoming GZIP, ZLIB, and raw DEFLATE payloads (supporting `kafka2x` / `k2http` compressed batches).
  - **Multi-Format Bulk Support**: Handles single JSON records, line-delimited records (NDJSON), JSON arrays (`[...]`), and concatenated JSON objects (`{...}{...}`).
  - **REST Path Routing & Sensor Extraction**: Parses REST URIs like `POST /rbdata/<sensor_uuid>/<topic>` (e.g., `POST /rbdata/bd2fe2c3-d0ff-4c27-aef9-122b698239b9/rb_flow`), extracting `sensor_uuid`, `proxy_uuid`, and the target Kafka topic dynamically.
- **UNIX Domain Sockets**: Supports both `unix` (Stream, SOCK_STREAM) and `unixgram` (Datagram, SOCK_DGRAM) sockets.
- **FIFO (Named Pipes)**: Continuously reads from Linux FIFOs with auto-reconnect handling for external writer processes.
- **Direct SNMP Trap Receiver**: Supports direct SNMP v1/v2c/v3 trap reception on UDP 162 (`snmptrap_udp`) with automatic numeric OID to MIB name translation using dynamic MIB directories (`mib_dirs`).
- **IP Security & Subnet Filtering**: Granular IP allowlisting (`allowed_ips`) and blacklisting (`blacklist_ips`) supporting single IPs and CIDR subnet blocks globally or per listener.
- **Message Splitting & Field Extraction**: Extract structured data from raw log strings using Key-Value parsing (`kv_extract`), CEF security headers (`cef_extract`), or custom named Regex patterns (`regex_patterns`).
- **Schema Standardization**: Map vendor-specific log keys (e.g., `src`, `srcip`, `source_ip`) to unified redBorder schema keys (`src_ip`, `dst_ip`, `src_port`, `dst_port`, `transport`, `username`) automatically via `schema_standardization`.
- **Dynamic & Static JSON Enrichment**: Flexible per-sensor lookup maps (`enrichment_map`) and conditional rules engine (`contains`, `match_key`) for tagging JSON payloads.

### `kafka2x` (Egress from Kafka to HTTP/REST)
- **Consumer Group Engine**: Subscribes to multiple Kafka topics (e.g. `rb_flow`, `rb_monitor`, `rb_syslog`, `rb_trap`) with automatic partition rebalancing and fault tolerance.
- **Topic-Qualified URL Dispatch**: Automatically appends the source Kafka topic to the destination REST path (e.g., posting to `/rbdata/<sensor_uuid>` automatically dispatches batches to `/rbdata/<sensor_uuid>/rb_flow`, `/rbdata/<sensor_uuid>/rb_monitor`, etc.).
- **RFC-Compliant Compression**: Clean stream compression supporting `"gzip"`, `"zlib"`, `"deflate"`, or `"none"` (guarantees valid Adler-32 / CRC32 checksum footers).
- **Framing Modes**:
  - `"newline"`: Standard newline-delimited JSON (NDJSON / `\n`).
  - `"concatenated"`: RedBorder streaming concatenated JSON (`{...}{...}`).
  - `"json_array"`: Standard JSON array bulk (`[ {...}, {...} ]`).
- **At-Least-Once Delivery**: Kafka consumer offsets are only committed after a successful HTTP 2xx response from the remote endpoint. If the destination is temporarily unavailable, workers retry with configurable backoff.
- **HTTP Connection Pooling**: Tuned `http.Transport` with persistent connection reuse and TLS verification settings.

---

## IP Security & Access Control (`x2kafka`)

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

---

## Configuration

### 1. `x2kafka` Ingress Config (`/etc/x2kafka/config.json`)

```json
{
  "debug": false,
  "brokers": ["kafka.service:9092"],
  "topic": "x2kafka_default",
  "num_workers": 16,
  "channel_buffer_size": 50000,
  "socket_buffer_bytes": 8388608,
  "stats_port": 7979,
  "stats_socket": "/run/x2kafka/x2kafka.sock",
  "listeners": [
    {
      "name": "syslog-collector",
      "proto": "syslog",
      "bind": "0.0.0.0",
      "port": 514,
      "mode": "syslog",
      "topic": "rb_syslog",
      "kv_extract": true,
      "cef_extract": true,
      "schema_standardization": true
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
      "topic": "rb_trap"
    }
  ]
}
```

### 2. `kafka2x` Egress Config (`/etc/kafka2x/config.json`)

Supports both JSON and legacy YAML format:

```json
{
  "debug": false,
  "kafka": {
    "brokers": ["kafka.service:9092"],
    "group_id": "kafka2x-http-forwarder",
    "topics": [
      "rb_flow",
      "rb_monitor",
      "rb_syslog",
      "rb_trap"
    ]
  },
  "pipeline": {
    "workers": 2,
    "batch_size": 1000,
    "timeout_ms": 100,
    "retries": 3,
    "backoff_sec": 2
  },
  "http": {
    "url": "https://http2k.redborder.cluster/rbdata/938c7905-807e-4b8f-b488-70d525ac933b",
    "method": "POST",
    "framing": "newline",
    "compression": "gzip",
    "insecure": false,
    "timeout": "15s"
  },
  "stats_port": 7989,
  "stats_socket": "/run/kafka2x/kafka2x.sock"
}
```

---

## CLI Usage & Monitoring

Both daemons offer CLI commands to query stats and run diagnostics:

### `x2kafka`
```bash
# Query live stats via UNIX socket or HTTP:
x2kafka -stats -c /etc/x2kafka/config.json

# Query stats directly with curl or netcat:
curl --unix-socket /run/x2kafka/x2kafka.sock http://localhost/stats
curl http://127.0.0.1:7979/stats
echo "stats" | nc -U /run/x2kafka/x2kafka.sock

# Run in debug mode:
x2kafka -d -c /etc/x2kafka/config.json
```

### `kafka2x`
```bash
# Query live stats via UNIX socket or HTTP:
kafka2x -stats -c /etc/kafka2x/config.json

# Query stats directly:
curl --unix-socket /run/kafka2x/kafka2x.sock http://localhost/stats
curl http://127.0.0.1:7989/stats
echo "stats" | nc -U /run/kafka2x/kafka2x.sock

# Run in debug mode:
kafka2x -d -c /etc/kafka2x/config.json
```

---

## Build & Test

Build both binaries into `bin/`:
```bash
make build
```

Run test suite across all packages:
```bash
make test
```

Code linting and vetting:
```bash
make lint
```

---

## Packaging (Separate RPMs)

Building the RPM produces two distinct packages:
1. `x2kafka-<version>-<release>.x86_64.rpm` (Daemon binary `/usr/bin/x2kafka`, `/etc/x2kafka/config.json`, `x2kafka.service`)
2. `kafka2x-<version>-<release>.x86_64.rpm` (Daemon binary `/usr/bin/kafka2x`, `/etc/kafka2x/config.json`, `kafka2x.service`)

Build packages using mock:
```bash
make rpm
```

---

## Systemd Service Management

Each daemon has its own isolated systemd service:

### Start & Enable `x2kafka`
```bash
sudo systemctl enable --now x2kafka
sudo systemctl status x2kafka
```

### Start & Enable `kafka2x`
```bash
sudo systemctl enable --now kafka2x
sudo systemctl status kafka2x
```

---

## Production Best Practice: Dedicated Services per Protocol

While `x2kafka` can run all listeners inside a single process, the **recommended production architecture** is to run dedicated systemd services for each protocol (`syslog`, `http2k`, `snmptrap`).

### Why Separate Services?
1. **Isolated Fault Domains**: A spike in Syslog traffic or a slow network client won't affect HTTP2K or SNMP Trap ingestion.
2. **Independent Restarts & Upgrades**: Restarting or reloading one collector (e.g. `systemctl restart x2kafka-syslog`) does not interrupt connections to other endpoints.
3. **Dedicated Tuning & Resource Limits**: Allows assigning custom CPU quotas, worker thread counts, memory limits (`MemoryMax=`), or scheduling priorities per service in systemd.
4. **Independent Monitoring**: Each service runs with its own telemetry port and IPC socket, allowing independent throughput and error tracking.

### Example Service Files & Configurations

Example systemd units and configs are provided in the repository:
- **Syslog Service (UDP/TCP 514)**:
  - Unit: [`resources/systemd/examples/x2kafka-syslog.service`](resources/systemd/examples/x2kafka-syslog.service)
  - Config: [`configs_example/x2kafka_syslog.json`](configs_example/x2kafka_syslog.json) (`stats_port: 7979`, `stats_socket: /run/x2kafka/syslog.sock`)
- **HTTP2K Service (HTTP 7980)**:
  - Unit: [`resources/systemd/examples/x2kafka-http2k.service`](resources/systemd/examples/x2kafka-http2k.service)
  - Config: [`configs_example/x2kafka_http2k.json`](configs_example/x2kafka_http2k.json) (`stats_port: 7980`, `stats_socket: /run/x2kafka/http2k.sock`)
- **Direct SNMP Trap Receiver (UDP 162)**:
  - Unit: [`resources/systemd/examples/x2kafka-snmptrap.service`](resources/systemd/examples/x2kafka-snmptrap.service)
  - Config: [`configs_example/x2kafka_snmptrap.json`](configs_example/x2kafka_snmptrap.json) (`stats_port: 7981`, `stats_socket: /run/x2kafka/snmptrap.sock`)

### How to Deploy Dedicated Services:

```bash
# 1. Copy the configuration files to /etc/x2kafka/
sudo cp configs_example/x2kafka_syslog.json /etc/x2kafka/syslog.json
sudo cp configs_example/x2kafka_http2k.json /etc/x2kafka/http2k.json
sudo cp configs_example/x2kafka_snmptrap.json /etc/x2kafka/snmptrap.json

# 2. Copy the example systemd unit files to /etc/systemd/system/
sudo cp resources/systemd/examples/x2kafka-syslog.service /etc/systemd/system/
sudo cp resources/systemd/examples/x2kafka-http2k.service /etc/systemd/system/
sudo cp resources/systemd/examples/x2kafka-snmptrap.service /etc/systemd/system/

# 3. Reload systemd and enable the services you require
sudo systemctl daemon-reload
sudo systemctl enable --now x2kafka-syslog
sudo systemctl enable --now x2kafka-http2k
sudo systemctl enable --now x2kafka-snmptrap
```

> [!IMPORTANT]
> When running multiple `x2kafka` instances concurrently on the same host, ensure that each configuration specifies a unique `stats_port` and `stats_socket` (as pre-configured in the example files) to prevent IPC socket or TCP port collisions.


