-- Analytical queries for Claude Code session data
-- All operate on claude_code.raw (path String, data JSON)
--
-- The path column contains the original file path.
-- The data column contains the raw JSON from each JSONL line.
--
-- Accessing JSON fields: data.field::Type (e.g. data.type::String)
-- Content blocks are Array(JSON): data.message.content::Array(JSON)
-- Use ARRAY JOIN to unnest content blocks.

-- 1. Overview
SELECT
    count() AS total_rows,
    countIf(data.type::String = 'user') AS user_messages,
    countIf(data.type::String = 'assistant') AS assistant_messages,
    countIf(data.type::String = 'progress') AS progress_updates,
    uniqExact(data.sessionId::String) AS sessions,
    uniqExact(replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1')) AS projects
FROM claude_code.raw;

-- 2. Daily activity
SELECT
    toDate(data.timestamp::DateTime64(3)) AS day,
    countIf(data.type::String = 'user') AS user_msgs,
    countIf(data.type::String = 'assistant') AS assistant_msgs,
    uniqExact(data.sessionId::String) AS sessions,
    sum(data.message.usage.output_tokens::UInt32) AS output_tokens
FROM claude_code.raw
WHERE data.type::String IN ('user', 'assistant')
GROUP BY day
ORDER BY day;

-- 3. Project activity
SELECT
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    countIf(data.type::String = 'user') AS user_msgs,
    countIf(data.type::String = 'assistant') AS assistant_msgs,
    uniqExact(data.sessionId::String) AS sessions,
    sum(data.message.usage.output_tokens::UInt32) AS output_tokens
FROM claude_code.raw
WHERE data.type::String IN ('user', 'assistant')
GROUP BY project
ORDER BY user_msgs DESC;

-- 4. Model usage
SELECT
    data.message.model::String AS model,
    count() AS messages,
    sum(data.message.usage.input_tokens::UInt64) AS input_tokens,
    sum(data.message.usage.output_tokens::UInt64) AS output_tokens,
    sum(data.message.usage.cache_read_input_tokens::UInt64) AS cache_read,
    sum(data.message.usage.cache_creation_input_tokens::UInt64) AS cache_creation,
    uniqExact(data.sessionId::String) AS sessions
FROM claude_code.raw
WHERE data.type::String = 'assistant'
    AND data.message.model::String NOT IN ('', '<synthetic>')
GROUP BY model
ORDER BY messages DESC;

-- 5. Tool usage ranking
SELECT
    block.name::String AS tool_name,
    count() AS calls,
    uniqExact(data.sessionId::String) AS sessions
FROM claude_code.raw
ARRAY JOIN data.message.content::Array(JSON) AS block
WHERE data.type::String = 'assistant'
    AND block.type::String = 'tool_use'
GROUP BY tool_name
ORDER BY calls DESC;

-- 6. Hourly distribution
SELECT
    toHour(data.timestamp::DateTime64(3)) AS hour,
    countIf(data.type::String = 'user') AS user_msgs,
    countIf(data.type::String = 'assistant') AS assistant_msgs
FROM claude_code.raw
WHERE data.type::String IN ('user', 'assistant')
GROUP BY hour
ORDER BY hour;

-- 7. Token usage per assistant turn (averages by model)
SELECT
    data.message.model::String AS model,
    count() AS turns,
    avg(data.message.usage.output_tokens::UInt32) AS avg_output,
    avg(data.message.usage.input_tokens::UInt32) AS avg_input,
    max(data.message.usage.output_tokens::UInt32) AS max_output,
    avg(data.message.usage.cache_read_input_tokens::UInt32) AS avg_cache_read
FROM claude_code.raw
WHERE data.type::String = 'assistant'
    AND data.message.model::String NOT IN ('', '<synthetic>')
GROUP BY model;

-- 8. Longest sessions
SELECT
    data.sessionId::String AS session_id,
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    min(data.timestamp::DateTime64(3)) AS started,
    max(data.timestamp::DateTime64(3)) AS ended,
    dateDiff('second', started, ended) AS duration_sec,
    countIf(data.type::String = 'user') AS user_msgs,
    countIf(data.type::String = 'assistant') AS assistant_msgs,
    sum(data.message.usage.output_tokens::UInt32) AS output_tokens
FROM claude_code.raw
WHERE data.type::String IN ('user', 'assistant')
GROUP BY session_id, project
ORDER BY duration_sec DESC
LIMIT 20;

-- 9. Most active sessions by output tokens
SELECT
    data.sessionId::String AS session_id,
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    countIf(data.type::String = 'user') AS user_msgs,
    countIf(data.type::String = 'assistant') AS assistant_msgs,
    sum(data.message.usage.output_tokens::UInt32) AS output_tokens
FROM claude_code.raw
WHERE data.type::String IN ('user', 'assistant')
GROUP BY session_id, project
ORDER BY output_tokens DESC
LIMIT 20;

-- 10. First question of each session (most recent)
SELECT
    data.sessionId::String AS session_id,
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    data.timestamp::DateTime64(3) AS ts,
    left(data.message.content::String, 150) AS question
FROM claude_code.raw
WHERE data.type::String = 'user'
    AND data.parentUuid::String = ''
    AND data.message.content::String != ''
ORDER BY ts DESC
LIMIT 30;

-- 11. Turn durations — slowest turns
SELECT
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    data.sessionId::String AS session_id,
    data.timestamp::DateTime64(3) AS ts,
    data.durationMs::UInt32 / 1000.0 AS duration_sec
FROM claude_code.raw
WHERE data.type::String = 'system' AND data.subtype::String = 'turn_duration'
ORDER BY data.durationMs::UInt32 DESC
LIMIT 20;

-- 12. Average turn duration by project
SELECT
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    avg(data.durationMs::UInt32) / 1000 AS avg_sec,
    max(data.durationMs::UInt32) / 1000 AS max_sec,
    count() AS turns
FROM claude_code.raw
WHERE data.type::String = 'system' AND data.subtype::String = 'turn_duration'
GROUP BY project
ORDER BY avg_sec DESC;

-- 13. Weekly token consumption
SELECT
    toMonday(data.timestamp::DateTime64(3)) AS week,
    sum(data.message.usage.input_tokens::UInt64) AS input_tokens,
    sum(data.message.usage.output_tokens::UInt64) AS output_tokens,
    sum(data.message.usage.cache_read_input_tokens::UInt64) AS cache_read,
    sum(data.message.usage.cache_creation_input_tokens::UInt64) AS cache_creation
FROM claude_code.raw
WHERE data.type::String = 'assistant'
GROUP BY week
ORDER BY week;

-- 14. PR links
SELECT
    data.prRepository::String AS repo,
    data.prNumber::UInt32 AS pr,
    data.prUrl::String AS url,
    data.timestamp::DateTime64(3) AS ts
FROM claude_code.raw
WHERE data.type::String = 'pr-link'
ORDER BY ts DESC;

-- 15. Session summaries
SELECT
    data.summary::String AS summary,
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project
FROM claude_code.raw
WHERE data.type::String = 'summary' AND data.summary::String != ''
ORDER BY path;

-- 16. Git branches worked on
SELECT
    data.gitBranch::String AS branch,
    replaceRegexpOne(path, '.*/projects/([^/]+)/.*', '\\1') AS project,
    countIf(data.type::String = 'user') AS user_msgs,
    uniqExact(data.sessionId::String) AS sessions
FROM claude_code.raw
WHERE data.gitBranch::String != '' AND data.type::String IN ('user', 'assistant')
GROUP BY branch, project
ORDER BY user_msgs DESC
LIMIT 30;

-- 17. Claude Code versions over time
SELECT
    data.version::String AS version,
    min(data.timestamp::DateTime64(3)) AS first_seen,
    max(data.timestamp::DateTime64(3)) AS last_seen,
    uniqExact(data.sessionId::String) AS sessions
FROM claude_code.raw
WHERE data.version::String != '' AND data.type::String = 'user'
GROUP BY version
ORDER BY first_seen;

-- 18. Cache hit ratio by day
SELECT
    toDate(data.timestamp::DateTime64(3)) AS day,
    sum(data.message.usage.cache_read_input_tokens::UInt64) AS cache_read,
    sum(data.message.usage.cache_creation_input_tokens::UInt64) AS cache_created,
    if(cache_read + cache_created > 0,
       round(cache_read * 100.0 / (cache_read + cache_created), 1), 0) AS cache_hit_pct
FROM claude_code.raw
WHERE data.type::String = 'assistant'
GROUP BY day
ORDER BY day;

-- 19. Bash commands used
SELECT
    block.input.command::String AS cmd,
    block.input.description::String AS descr,
    count() AS times
FROM claude_code.raw
ARRAY JOIN data.message.content::Array(JSON) AS block
WHERE data.type::String = 'assistant'
    AND block.type::String = 'tool_use'
    AND block.name::String = 'Bash'
GROUP BY cmd, descr
ORDER BY times DESC
LIMIT 30;

-- 20. Files most frequently read/edited
SELECT
    block.input.file_path::String AS file,
    block.name::String AS tool,
    count() AS times
FROM claude_code.raw
ARRAY JOIN data.message.content::Array(JSON) AS block
WHERE data.type::String = 'assistant'
    AND block.type::String = 'tool_use'
    AND block.name::String IN ('Read', 'Edit', 'Write')
GROUP BY file, tool
ORDER BY times DESC
LIMIT 30;

-- 21. Grep patterns — what are you searching for?
SELECT
    block.input.pattern::String AS pattern,
    count() AS times,
    uniqExact(data.sessionId::String) AS sessions
FROM claude_code.raw
ARRAY JOIN data.message.content::Array(JSON) AS block
WHERE data.type::String = 'assistant'
    AND block.type::String = 'tool_use'
    AND block.name::String = 'Grep'
GROUP BY pattern
ORDER BY times DESC
LIMIT 30;

-- 22. Task/subagent usage
SELECT
    block.input.subagent_type::String AS subagent,
    left(block.input.prompt::String, 100) AS prompt_preview,
    count() AS times
FROM claude_code.raw
ARRAY JOIN data.message.content::Array(JSON) AS block
WHERE data.type::String = 'assistant'
    AND block.type::String = 'tool_use'
    AND block.name::String = 'Task'
GROUP BY subagent, prompt_preview
ORDER BY times DESC
LIMIT 30;

-- 23. History entries (prompts pasted to sessions)
SELECT
    toDateTime(data.timestamp::UInt64 / 1000) AS ts,
    data.project::String AS project,
    left(data.display::String, 200) AS prompt
FROM claude_code.raw
WHERE path LIKE '%history.jsonl'
ORDER BY ts DESC
LIMIT 20;
