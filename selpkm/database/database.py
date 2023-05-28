import datetime
import importlib
import pathlib
import sqlite3


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
            migration_id = int(migration.split('_')[0])
            finished_migrations = self._migrations()
            if migration_id in finished_migrations:
                continue
            module_string = f'{package}.migrations.{migration}'
            module = importlib.import_module(module_string)
            migrate = getattr(module, 'migrate')
            if migrate:
                timestamp = datetime.datetime.now(datetime.timezone.utc)
                timestamp_str = datetime.datetime.isoformat(timestamp)
                with self._connection as conn:
                    migrate(conn, current_migrations=finished_migrations)
                    conn.execute('INSERT INTO versions VALUES (?,?)',
                                 (migration_id, timestamp_str))

    def _initialize(self):
        with self._connection as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS versions ('
                         '    version_id INTEGER PRIMARY KEY,'
                         '    timestamp TEXT'
                         ')')

    def _migrations(self) -> list[int]:
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

    def get_tables(self) -> list[str]:
        with self._connection as conn:
            result = conn.execute(
                'SELECT name FROM sqlite_master WHERE type="table"')
        all_tables = result.fetchall()
        return [a[0] for a in all_tables]

    def get_version(self) -> int | None:
        all_versions = self._migrations()
        if not all_versions:
            return None
        else:
            return all_versions[-1]
