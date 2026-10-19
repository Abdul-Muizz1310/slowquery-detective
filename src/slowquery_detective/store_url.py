"""Validate and normalise the Postgres store URL — spec 07 cases 17, 43.

Neon hands out libpq-flavoured URLs (``postgresql://…?sslmode=require&
channel_binding=require``). SQLAlchemy's asyncpg dialect passes query
parameters to ``asyncpg.connect`` as keyword arguments, and asyncpg rejects
both ``sslmode`` and ``channel_binding``. So:

- ``postgres://`` / ``postgresql://`` select the asyncpg driver;
- for asyncpg, ``sslmode=X`` becomes ``ssl=X`` (unless ``ssl`` is already
  set) and ``channel_binding`` is dropped;
- ``postgresql+psycopg://`` (psycopg 3, async-capable, libpq semantics) is
  kept as given;
- anything else — another database, a sync-only driver — is rejected.

Error messages never include the URL, which carries the password.
"""

from __future__ import annotations

from typing import Final
from urllib.parse import parse_qsl, urlencode, urlsplit

_ASYNCPG: Final = "postgresql+asyncpg"
_PSYCOPG: Final = "postgresql+psycopg"
_TO_ASYNCPG: Final[frozenset[str]] = frozenset({"postgres", "postgresql", _ASYNCPG})


def _join(scheme: str, netloc: str, path: str, query: str, fragment: str) -> str:
    # urlunsplit drops the "//" of an empty netloc (a unix-socket URL), which
    # SQLAlchemy would then misread; join by hand.
    out = f"{scheme}://{netloc}{path}"
    if query:
        out += f"?{query}"
    if fragment:
        out += f"#{fragment}"
    return out


def normalise_store_url(url: str) -> str:
    """Return a URL ``create_async_engine`` can open, or raise ``ValueError``."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError("store_url must be a non-empty Postgres URL")
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in _TO_ASYNCPG and scheme != _PSYCOPG:
        raise ValueError(
            "store_url must use postgres://, postgresql://, postgresql+asyncpg:// "
            f"or postgresql+psycopg:// (got scheme {scheme!r})"
        )
    if not parts.netloc and parts.path in {"", "/"}:
        raise ValueError("store_url names no host and no database")
    if scheme == _PSYCOPG:
        return _join(scheme, parts.netloc, parts.path, parts.query, parts.fragment)

    query = parts.query
    pairs = parse_qsl(query, keep_blank_values=True)
    if any(k in {"sslmode", "channel_binding"} for k, _ in pairs):
        has_ssl = any(k == "ssl" for k, _ in pairs)
        kept: list[tuple[str, str]] = []
        for key, value in pairs:
            if key == "channel_binding":
                continue
            if key == "sslmode":
                if not has_ssl:
                    kept.append(("ssl", value))
                    has_ssl = True
                continue
            kept.append((key, value))
        query = urlencode(kept)
    return _join(_ASYNCPG, parts.netloc, parts.path, query, parts.fragment)
