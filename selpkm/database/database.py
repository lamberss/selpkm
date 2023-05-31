import importlib
import pathlib
import sqlite3
from typing import List, Union

from .util import get_tables, get_timestamp


class Database(object):
    def __init__(self, path: str or pathlib.Path, initialize: bool = True):
        self._path = path
        self._connection = sqlite3.connect(str(self._path))

        if initialize:
            if self.get_version() is None:
                self._initialize()
            self._apply_migrations()

    def __del__(self):
        self._connection.close()

    def _apply_migrations(self):
        from .migrations import all_migrations
        package = '.'.join(__name__.split('.')[:-1])
        for migration in all_migrations:
            migration_id = int(migration.split('_')[1])
            finished_migrations = self._migrations()
            if migration_id in finished_migrations:
                continue
            module_string = f'{package}.migrations.{migration}'
            module = importlib.import_module(module_string)
            migrate = getattr(module, 'migrate')
            if migrate:
                timestamp = get_timestamp()
                with self._connection as conn:
                    migrate(conn, current_migrations=finished_migrations)
                    conn.execute('INSERT INTO versions VALUES (?,?)',
                                 (migration_id, timestamp))

    def _initialize(self):
        with self._connection as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS versions ('
                         '    version_id INTEGER PRIMARY KEY,'
                         '    timestamp TEXT'
                         ')')

    def _migrations(self) -> List[int]:
        all_versions = []
        try:
            with self._connection as conn:
                result = conn.execute('SELECT * FROM versions')
        except sqlite3.OperationalError:
            return []
        else:
            all_versions = result.fetchall()
            if not all_versions:
                return []
            else:
                return [a[0] for a in sorted(all_versions)]

    def get_tables(self) -> List[str]:
        return get_tables(self._connection)

    def get_version(self) -> Union[int, None]:
        all_versions = self._migrations()
        if not all_versions:
            return None
        else:
            return all_versions[-1]
