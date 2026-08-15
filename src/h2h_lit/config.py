"""Environment-backed configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    semantic_scholar_api_key: str | None = None
    openai_api_key: str | None = None
    unpaywall_email: str | None = None
    user_agent: str = "H2H-Lit-Pipeline/0.1"


def load_settings() -> Settings:
    return Settings(
        semantic_scholar_api_key=os.getenv("SEMANTIC_SCHOLAR_API_KEY") or None,
        openai_api_key=os.getenv("OPENAI_API_KEY") or None,
        unpaywall_email=os.getenv("UNPAYWALL_EMAIL") or None,
        user_agent=os.getenv("H2H_USER_AGENT") or "H2H-Lit-Pipeline/0.1",
    )

