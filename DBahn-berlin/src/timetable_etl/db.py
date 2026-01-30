from __future__ import annotations
import psycopg2
from psycopg2.extensions import connection as PgConnection
from .config import Settings

# This is just a helpe function to create a connection to the local database
def connect(settings: Settings) -> PgConnection:
    return psycopg2.connect(
        host=settings.db_host,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
