# Development

Run tests locally before attempting live API calls:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[dev]"
pytest
```

Live PubMed, Europe PMC, CrossRef, Semantic Scholar, Unpaywall, PDF download, and LLM
calls are not part of the initial checkpoint.

Phase 2 keeps adapters offline-testable by requiring an injected HTTP client. Unit tests
use fakes/mocks; production network clients will be enabled only in a later checkpoint.
