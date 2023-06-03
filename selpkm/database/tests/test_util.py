import datetime
import sqlite3
import unittest

from ..util import get_tables, get_timestamp


class TestDatabaseUtil(unittest.TestCase):
    def test_get_tables(self):
        conn = sqlite3.connect(':memory:')
        with conn:
            conn.execute('CREATE TABLE test1 (test1_id INTEGER PRIMARY KEY)')
            conn.execute('CREATE TABLE test2 (test2_id INTEGER PRIMARY KEY)')
        self.assertEqual(get_tables(conn), ['test1', 'test2'])

    def test_get_tables_empty(self):
        conn = sqlite3.connect(':memory:')
        self.assertEqual(get_tables(conn), [])

    def test_get_timestamp(self):
        dt = datetime.datetime(2000, 3, 14, 16, 21, 32)
        dts = "2000-03-14T16:21:32"
        self.assertEqual(get_timestamp(dt), dts)

        dt = datetime.datetime(2000, 3, 14, 16, 21, 32, 5)
        dts = "2000-03-14T16:21:32.000005"
        self.assertEqual(get_timestamp(dt), dts)

        dt = datetime.datetime(2000, 3, 14, 16, 21, 32,
                               tzinfo=datetime.timezone.utc)
        dts = "2000-03-14T16:21:32+00:00"
        self.assertEqual(get_timestamp(dt), dts)

        dt = datetime.datetime.now(datetime.timezone.utc)
        gt = datetime.datetime.fromisoformat(get_timestamp())
        self.assertLessEqual(dt - gt, datetime.timedelta(seconds=1))
