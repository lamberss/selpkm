import sqlite3
import unittest

from ...util import get_tables

from ..migration_01_initial_db import migrate


class TestMigration01InitialDB(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def tearDown(self):
        self.conn.close()

    def test_get_tables(self):
        self.assertEqual(get_tables(self.conn), [])
        with self.conn:
            migrate(self.conn)
        self.assertEqual(get_tables(self.conn), ['containers', 'notes'])
