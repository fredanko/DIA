from __future__ import annotations

import psycopg2
from psycopg2.extensions import connection as PgConnection

from .config import Settings

def connect(settings: Settings) -> PgConnection:
    """Create a psycopg2 connection using Settings (no autocommit)."""
    return psycopg2.connect(
        host=settings.db_host,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
