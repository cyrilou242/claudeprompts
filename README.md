## Read the blog: [https://clickhouse.com/blog/agentic-coding](https://clickhouse.com/blog/agentic-coding)

### Claude Prompts

A viewer of Claude Code sessions with ClickHouse backend.

![Claude Prompts](screenshot.png)

### Schema setup

Create a ClickHouse database and tables:

```sql
CREATE DATABASE IF NOT EXISTS claude_code;

CREATE TABLE IF NOT EXISTS claude_code.raw
(
    path String,
    data JSON,
    metadata JSON
)
ENGINE = ReplacingMergeTree
ORDER BY (data.sessionId::String, data.timestamp::String, data.uuid::String);

CREATE TABLE IF NOT EXISTS claude_code.classification
(
    data JSON
)
ENGINE = ReplacingMergeTree
ORDER BY data.session_id::String;
```

### Ingestion tool

The Go tool in `cmd/ingest/` watches `~/.claude/` and continuously pipes new session data into ClickHouse. It uses `clickhouse-local` to read JSONL files and streams them via `clickhouse-client`.

**Build:**

```bash
go build -o ingest ./cmd/ingest/
```

**Run once** (one-off upload):

```bash
./ingest --once --host localhost --password 'your-password'
```

**Run continuously** (default: scans every 60s):

```bash
./ingest --host localhost --password 'your-password'
```

**Flags and environment variables:**

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `-host` | `CH_HOST` | `localhost` | ClickHouse host |
| `-port` | `CH_PORT` | `9000` | ClickHouse native port |
| `-user` | `CH_USER` | `default` | ClickHouse user |
| `-password` | `CH_PASSWORD` | | ClickHouse password |
| `-database` | `CH_DATABASE` | `claude_code` | ClickHouse database |
| `-table` | `CH_TABLE` | `raw` | ClickHouse table |
| `-secure` | `CH_SECURE` | off | Use TLS |
| `-claude-dir` | `CLAUDE_DIR` | `~/.claude` | Claude data directory |
| `-interval` | `SCAN_INTERVAL` | `1m` | Scan interval |
| `-metadata` | `CH_METADATA` | `{}` | JSON metadata attached to every row |
| `-once` | | `false` | Run once and exit |

### Auto-start on login (`.bash_profile` example)

Add this to your `~/.bash_profile` to build and start the ingestion tool in the background on every new shell session:

```bash
export CH_HOST=your-instance.clickhouse.cloud
export CH_PASSWORD=your-password

# Auto-start claudeprompts ingestion (lock file prevents duplicate instances)
(cd /path/to/claudeprompts && go build -o ingest ./cmd/ingest/ 2>/dev/null && \
 nohup ./ingest --host "$CH_HOST" --port 9440 --secure --password "$CH_PASSWORD" \
   --metadata '{"user":"your-name"}' >> /tmp/claudeprompts-ingest.log 2>&1 &)
```

For a local ClickHouse instance, use `--host localhost --port 9000` and drop `--secure`.

### Viewer

Open `index.html` in a browser (or use the [GitHub Pages site](https://cyrilou242.github.io/claudeprompts/)). You'll be prompted to enter your ClickHouse connection details (URL, user, password, database). Credentials are stored in your browser's localStorage for convenience.

### One-off upload (without the Go tool)

You can also upload sessions manually with `clickhouse-local` and `clickhouse-client`:

```bash
clickhouse-local -q "
    SELECT _path AS path, json AS data
    FROM file('$HOME/.claude/projects/*/*.jsonl', 'JSONAsString')
    WHERE isValidJSON(json)
    SETTINGS input_format_allow_errors_ratio=0.1
    FORMAT Native
" | clickhouse-client --host '...' --password '...' -q "INSERT INTO claude_code.raw FORMAT Native"
```

### Do not share your sessions publicly

The sessions contain tool calls and results, including fragments of arbitrary files. They may contain sensitive info — it's not recommended to share sessions publicly.
