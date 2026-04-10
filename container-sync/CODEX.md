# CODEX Working Notes

- Primary goal: improve `sync.py` container sync performance through parallel processing.
- Prefer the existing Python 2 style used in Swift. Avoid introducing Python 3-only style such as type hints unless explicitly requested.
- Do not apply code changes immediately. Propose the change first, get user confirmation, then reflect it in code.
- In `sync_parallel_v*.py`, wrap every code addition relative to the original `sync.py` with Korean marker comments: `# 추가된 부분 시작: <reason>` before the added block and `# 추가된 부분 끝: <reason>` after it.
