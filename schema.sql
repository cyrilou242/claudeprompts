-- Schema for Claude Code session analytics
-- One table, raw JSON, all analysis via SQL.
--
-- Step 1: Create table
--   clickhouse-client < schema.sql
--
-- Step 2: Load data:
--   clickhouse-local -q "SELECT _path AS path, json AS data FROM file('~/.claude/projects/*/*.jsonl', 'JSONAsString') WHERE isValidJSON(json) SETTINGS input_format_allow_errors_ratio=0.1 FORMAT Native" | clickhouse-client -q "INSERT INTO claude_code.raw FORMAT Native"
--   clickhouse-local -q "SELECT _path AS path, json AS data FROM file('~/.claude/history.jsonl', 'JSONAsString') WHERE isValidJSON(json) SETTINGS input_format_allow_errors_ratio=0.1 FORMAT Native" | clickhouse-client -q "INSERT INTO claude_code.raw FORMAT Native"

CREATE DATABASE IF NOT EXISTS claude_code;

CREATE TABLE IF NOT EXISTS claude_code.raw
(
    path String,
    data JSON
)
ENGINE = ReplacingMergeTree
ORDER BY (data.sessionId::String, data.timestamp::String, data.uuid::String);
