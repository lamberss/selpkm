import sqlite3


def migrate(connection: sqlite3.Connection, current_migrations: list[int] | None = None):
    connection.execute('CREATE TABLE IF NOT EXISTS containers ('
                       '    container_id INTEGER PRIMARY KEY,'
                       '    parent_id INTEGER NULL REFERENCES containers(container_id) ON DELETE CASCADE,'
                       '    created TEXT,'
                       '    modified TEXT'
                       ')')
    connection.execute('CREATE TABLE IF NOT EXISTS notes ('
                       '    note_id INTEGER PRIMARY KEY,'
                       '    container_id INTEGER,'
                       '    created TEXT,'
                       '    modified TEXT,'
                       '    FOREIGN KEY(container_id) REFERENCES containers(container_id) ON DELETE CASCADE'
                       ')')
