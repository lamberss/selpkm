import importlib
import pathlib
import sqlite3
from typing import List, Union

from .util import get_tables, get_timestamp


class DatabaseException(Exception):
    pass


class RowDoesNotExist(DatabaseException):
    entity = 'Row'

    def __init__(self,
                 id: Union[int, None] = None,
                 name: Union[str, int] = None):
        self.id = id
        self.name = name
        labels = []
        if id is not None:
            labels.append(f'id={self.id}')
        if name is not None:
            labels.append(f'name="{self.name}"')
        label_str = ''
        if labels:
            label_str = f'with ({",".join(labels)}) '
        message = f'{self.entity} {label_str}does not exist.'
        self.message = message
        super().__init__(message, id, name)

    def __str__(self):
        return str(self.message)


class ContainerDoesNotExist(RowDoesNotExist):
    entity = 'Container'


class ContainerNameExists(DatabaseException):
    def __init__(self, name):
        self.name = name
        message = f'Cannot add container named "{name}", it already exists.'
        super().__init__(message)


class NoteDoesNotExist(RowDoesNotExist):
    entity = 'Note'


class Database(object):
    def __init__(self,
                 path: Union[str, pathlib.Path],
                 initialize: bool = True):
        self._path = path
        self._connection = sqlite3.connect(str(self._path))
        self._connection.execute('PRAGMA foreign_keys = 1')
        self._connection.row_factory = sqlite3.Row

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

    def _get_row(self,
                 row_name: str,
                 exception: DatabaseException,
                 id: Union[int, None] = None,
                 name: Union[str, None] = None) -> dict:
        sql = f'SELECT * FROM {row_name}s'
        sql_where = []
        params = []
        if id is not None:
            sql_where.append(f'{row_name}_id=?')
            params.append(id)
        if name is not None:
            sql_where.append('name=?')
            params.append(name)
        if sql_where:
            sql = f'{sql} WHERE {" AND ".join(sql_where)}'
        with self._connection as conn:
            result = conn.execute(sql, tuple(params))
        rows = result.fetchall()
        if not rows or len(rows) > 1:
            raise exception(id=id, name=name)
        return dict(rows[0])

    def _get_rows(self,
                  row_name: str,
                  where: str = '') -> List[dict]:
        sql = f'SELECT * FROM {row_name}'
        if where:
            sql += f' WHERE {where}'
        with self._connection as conn:
            result = conn.execute(sql)
        rows = result.fetchall()
        if rows is None:
            return []
        return [dict(r) for r in rows]

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
                return [a['version_id'] for a in sorted(all_versions)]

    def add_container(self,
                      name: str,
                      parent_id: Union[int, None] = None,
                      parent_name: Union[str, None] = None) -> int:
        timestamp = get_timestamp()
        if parent_id is not None or parent_name is not None:
            parent = self.get_container(id=parent_id, name=parent_name)
        else:
            parent = {'container_id': None}
        try:
            with self._connection as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '
                               'containers(name,parent_id,created,modified) '
                               'VALUES(?,?,?,?)',
                               (name, parent['container_id'], timestamp,
                                timestamp))
        except sqlite3.IntegrityError as exception:
            if exception.args[0] == 'UNIQUE constraint failed: containers.name':
                raise ContainerNameExists(name)
            elif exception.args[0] == 'FOREIGN KEY constraint failed':
                raise ContainerDoesNotExist(parent['container_id'])
            else:
                raise exception
        return cursor.lastrowid

    def add_note(self,
                 name: str,
                 description: Union[str, None] = None,
                 container_id: Union[int, None] = None,
                 container_name: Union[str, None] = None) -> int:
        timestamp = get_timestamp()
        container = self.get_container(id=container_id, name=container_name)
        try:
            with self._connection as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '
                               'notes(name,description,container_id,created,'
                               'modified) '
                               ' VALUES(?,?,?,?,?)',
                               (name, description, container['container_id'],
                                timestamp, timestamp))
        except sqlite3.IntegrityError as exception:
            if exception.args[0] == 'FOREIGN KEY constraint failed':
                raise ContainerDoesNotExist(container['container_id'])
            else:
                raise exception
        return cursor.lastrowid

    def get_container(self,
                      id: Union[int, None] = None,
                      name: Union[str, None] = None) -> dict:
        return self._get_row('container', ContainerDoesNotExist, id=id,
                             name=name)

    def get_containers(self) -> List[dict]:
        return self._get_rows('containers')

    def get_note(self, id: Union[int, None] = None) -> dict:
        return self._get_row('note', NoteDoesNotExist, id=id)

    def get_notes(self,
                  container_id: Union[int, None] = None,
                  container_name: Union[str, None] = None) -> List[dict]:
        where = ''
        if container_id is not None or container_name is not None:
            container = self.get_container(id=container_id,
                                           name=container_name)
            where = f'container_id={container["container_id"]}'
        return self._get_rows('notes', where=where)

    def get_tables(self) -> List[str]:
        return get_tables(self._connection)

    def get_version(self) -> Union[int, None]:
        all_versions = self._migrations()
        if not all_versions:
            return None
        else:
            return all_versions[-1]
