import sqlite3
from typing import List, Union


def migrate(connection: sqlite3.Connection, current_migrations: Union[List[int], None] = None):
    connection.execute('CREATE TABLE IF NOT EXISTS containers ('
                       '    container_id INTEGER PRIMARY KEY,'
                       '    name TEXT NOT NULL,'
                       '    parent_id INTEGER NULL REFERENCES containers(container_id) ON DELETE CASCADE,'
                       '    created TEXT NOT NULL,'
                       '    modified TEXT NOT NULL'
                       ')')
    connection.execute('CREATE TABLE IF NOT EXISTS notes ('
                       '    note_id INTEGER PRIMARY KEY,'
                       '    container_id INTEGER NOT NULL,'
                       '    created TEXT NOT NULL,'
                       '    modified TEXT NOT NULL,'
                       '    FOREIGN KEY(container_id) REFERENCES containers(container_id) ON DELETE CASCADE'
                       ')')
