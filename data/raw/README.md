# Raw landing zone

Files exactly as received, named `<source_id>_YYYYMMDD_HHMMSS.<ext>` (UTC), each with a
`.meta.json` sidecar (URL, HTTP headers, SHA-256). Git-ignored: regenerate with
`python scripts/fetch_samples.py` (samples) or `python ingest.py --mode historical` (Workshop #2).
