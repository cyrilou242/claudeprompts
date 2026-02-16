#!/usr/bin/env python3
"""
Classify Claude Code sessions using Claude Haiku via subagent-style batched prompts.
Reads sessions_content.json, outputs session_classifications.jsonl
"""

import json
import sys
import os

INPUT = os.path.join(os.path.dirname(__file__), 'sessions_content.json')
OUTPUT = os.path.join(os.path.dirname(__file__), 'session_classifications.jsonl')

def load_sessions():
    with open(INPUT) as f:
        return json.load(f)

def load_existing():
    """Load already-classified session IDs to allow resuming."""
    done = set()
    if os.path.exists(OUTPUT):
        with open(OUTPUT) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        obj = json.loads(line)
                        done.add(obj['session_id'])
                    except:
                        pass
    return done

def build_prompt(batch):
    """Build classification prompt for a batch of sessions."""
    prompt = """Classify each session below according to these criteria. For each session, output a JSON object on a single line with these fields:

- session_id: the session ID
- remarkable: true/false - is this session particularly remarkable, interesting, or unusual?
- remarkable_reason: brief explanation if remarkable, empty string otherwise
- user_corrected_agent: true/false - did the user correct the agent and prove it wrong? Was the agent surprised?
- correction_detail: brief description of what the correction was about, empty string if none
- agent_was_right: true/false - when user argued with agent, was the agent actually right?
- agent_right_detail: brief description, empty string if none
- sensitive_info: true/false - does the session contain sensitive personal info about the user or sensitive data like credentials, API keys, passwords?
- sensitive_detail: brief description of what sensitive info, empty string if none

Output ONLY the JSON lines, one per session, no other text. If a session is too short or trivial to classify, set all boolean fields to false.

Sessions to classify:

"""
    for s in batch:
        prompt += f"=== SESSION {s['session_id']} ===\n"
        prompt += f"Project: {s['project']}\n"
        if s.get('summary'):
            prompt += f"Summary: {s['summary']}\n"
        prompt += f"User messages: {s['user_msgs']}\n"
        prompt += f"{s['conversation']}\n\n"

    return prompt

if __name__ == '__main__':
    sessions = load_sessions()
    # Filter to meaningful sessions
    sessions = [s for s in sessions if len(s.get('conversation', '')) > 50]

    done = load_existing()
    remaining = [s for s in sessions if s['session_id'] not in done]

    print(f"Total meaningful sessions: {len(sessions)}")
    print(f"Already classified: {len(done)}")
    print(f"Remaining: {len(remaining)}")

    if not remaining:
        print("All sessions classified!")
        sys.exit(0)

    # Build batches of 20
    BATCH_SIZE = 20
    batches = [remaining[i:i+BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    print(f"Batches to process: {len(batches)}")

    # Output the prompts for processing
    os.makedirs('/tmp/classify_batches', exist_ok=True)
    for i, batch in enumerate(batches):
        prompt = build_prompt(batch)
        with open(f'/tmp/classify_batches/batch_{i:03d}.txt', 'w') as f:
            f.write(prompt)

    print(f"Wrote {len(batches)} batch prompts to /tmp/classify_batches/")
    print(f"Total chars: {sum(len(build_prompt(b)) for b in batches)}")
