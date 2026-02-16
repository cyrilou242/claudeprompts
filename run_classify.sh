#!/bin/bash
# Process classification batches sequentially, appending results
OUTPUT="/home/milovidov/work/alexeyprompts/claude_code_analytics/session_classifications.jsonl"

for batch_file in /tmp/classify_batches/batch_*.txt; do
    batch_name=$(basename "$batch_file" .txt)
    echo "Processing $batch_name..."
    # Count sessions in this batch
    count=$(grep -c "^=== SESSION" "$batch_file")
    echo "  Sessions: $count"
done

echo "Total batch files: $(ls /tmp/classify_batches/batch_*.txt | wc -l)"
