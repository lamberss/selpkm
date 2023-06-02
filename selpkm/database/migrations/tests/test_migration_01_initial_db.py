import sqlite3
import unittest

from ...util import get_tables

from ..migration_01_initial_db import migrate


class TestMigration01InitialDB(unittest.TestCase):
    def apply(self):
        with self.conn:
            migrate(self.conn)

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.execute('PRAGMA foreign_keys = 1')

    def tearDown(self):
        self.conn.close()

    def test_add_container(self):
        self.conn.row_factory = sqlite3.Row
        self.apply()
        with self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO containers VALUES(2,"thing2",1,"now","now")')
        with self.conn:
            result = self.conn.execute('SELECT * FROM containers')
        self.assertEqual([dict(r) for r in result.fetchall()],
                         [{'container_id': 1, 'name': 'thing1', 'parent_id': None,
                           'created': 'now', 'modified': 'now'},
                          {'container_id': 2, 'name': 'thing2', 'parent_id': 1,
                           'created': 'now', 'modified': 'now'}])

    def test_add_container_null_name(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,NULL,NULL,"now","now")')

    def test_add_container_null_created(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,NULL,"now")')

    def test_add_container_null_modified(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now",NULL)')

    def test_add_note(self):
        self.conn.row_factory = sqlite3.Row
        self.apply()
        with self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(1,"note1","description1",1,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(2,"note2",NULL,1,"now","now")')
        with self.conn:
            result = self.conn.execute('SELECT * FROM notes')
        self.assertEqual([dict(r) for r in result.fetchall()],
                         [{'note_id': 1, 'name': 'note1',
                           'description': 'description1', 'container_id': 1,
                           'created': 'now', 'modified': 'now'},
                          {'note_id': 2, 'name': 'note2',
                           'description': None, 'container_id': 1,
                           'created': 'now', 'modified': 'now'}])

    def test_add_note_bad_container(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(1,"note1","description1",2,"now","now")')

    def test_add_note_null_created(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(1,"note1","description1",1,NULL,"now")')

    def test_add_note_null_modified(self):
        self.apply()
        with self.assertRaises(sqlite3.IntegrityError), self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(1,"note1","description1",1,"now",NULL)')

    def test_delete_parent_container_cascades(self):
        self.apply()
        with self.conn:
            self.conn.execute('INSERT INTO containers VALUES(1,"thing1",NULL,"now","now")')
            self.conn.execute('INSERT INTO containers VALUES(2,"thing2",1,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(1,"note1",NULL,1,"now","now")')
            self.conn.execute('INSERT INTO notes VALUES(2,"note2",NULL,2,"now","now")')
        with self.conn:
            self.conn.execute('DELETE FROM containers WHERE container_id=1')
        with self.conn:
            result = self.conn.execute('SELECT * FROM containers')
        self.assertEqual(result.fetchall(), [])
        with self.conn:
            result = self.conn.execute('SELECT * FROM notes')
        self.assertEqual(result.fetchall(), [])

    def test_tables(self):
        self.assertEqual(get_tables(self.conn), [])
        self.apply()
        self.assertEqual(get_tables(self.conn), ['containers', 'notes'])