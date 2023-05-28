import unittest

from ..database import Database


class TestDatabase(unittest.TestCase):
    def test_get_tables(self):
        db = Database(':memory:')
        self.assertEqual(db.get_tables(), ['versions', 'containers', 'notes'])

    def test_get_tables_empty(self):
        db = Database(':memory:', initialize=False)
        self.assertEqual(db.get_tables(), [])

    def test_get_version(self):
        db = Database(':memory:')
        self.assertEqual(db.get_version(), 1)

    def test_get_version_none(self):
        db = Database(':memory:', initialize=False)
        self.assertIsNone(db.get_version())
