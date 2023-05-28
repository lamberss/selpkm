from datetime import datetime, timezone
import sqlite3


def get_tables(conn: sqlite3.Connection) -> list[str]:
    result = conn.execute('SELECT name FROM sqlite_master WHERE type="table"')
    all_tables = result.fetchall()
    return [a[0] for a in all_tables]


def get_timestamp(timestamp: datetime = datetime.now(timezone.utc)) -> str:
    return datetime.isoformat(timestamp)
