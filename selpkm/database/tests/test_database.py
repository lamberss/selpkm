import sqlite3
import unittest

from ..database import Database, ContainerDoesNotExist, ContainerNameExists, \
    NoteDoesNotExist


class TestDatabase(unittest.TestCase):
    def test_add_container(self):
        db = Database(':memory:')
        self.assertEqual(db.add_container('thing 1'), 1)
        self.assertEqual(db.add_container('thing 2'), 2)
        self.assertEqual(db.add_container('thing 3', parent_id=1), 3)
        self.assertEqual(db.add_container('thing 4', parent_name='thing 1'), 4)
        with self.assertRaises(ContainerNameExists):
            db.add_container('thing 2')
        with self.assertRaises(ContainerDoesNotExist):
            db.add_container('thing 999', parent_id=999)
        with self.assertRaises(ContainerDoesNotExist):
            db.add_container('thing 5', parent_id=1, parent_name='thing 2')
        with db._connection as conn:
            result = conn.execute(
                'SELECT container_id, name, parent_id FROM containers')
        self.assertEqual([dict(r) for r in result.fetchall()],
                         [{'container_id': 1, 'name': 'thing 1',
                           'parent_id': None},
                          {'container_id': 2, 'name': 'thing 2',
                           'parent_id': None},
                          {'container_id': 3, 'name': 'thing 3',
                           'parent_id': 1},
                          {'container_id': 4, 'name': 'thing 4',
                           'parent_id': 1}])

    def test_get_container(self):
        db = Database(':memory:')
        with db._connection as conn:
            conn.execute(
                'INSERT INTO containers VALUES(1,"thing 1",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(2,"thing 2",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(3,"thing 3",1,"now","now")')
        thing1 = {'container_id': 1, 'name': 'thing 1', 'parent_id': None,
                  'created': 'now', 'modified': 'now'}
        thing2 = {'container_id': 2, 'name': 'thing 2', 'parent_id': None,
                  'created': 'now', 'modified': 'now'}
        thing3 = {'container_id': 3, 'name': 'thing 3', 'parent_id': 1,
                  'created': 'now', 'modified': 'now'}
        self.assertEqual(db.get_container(id=1), thing1)
        self.assertEqual(db.get_container(name='thing 1'), thing1)
        self.assertEqual(db.get_container(id=1, name='thing 1'), thing1)
        self.assertEqual(db.get_container(id=2), thing2)
        self.assertEqual(db.get_container(name='thing 2'), thing2)
        self.assertEqual(db.get_container(id=2, name='thing 2'), thing2)
        self.assertEqual(db.get_container(id=3), thing3)
        self.assertEqual(db.get_container(name='thing 3'), thing3)
        self.assertEqual(db.get_container(id=3, name='thing 3'), thing3)
        with self.assertRaises(ContainerDoesNotExist):
            db.get_container(id=99)
        with self.assertRaises(ContainerDoesNotExist):
            db.get_container(name='not a container')
        with self.assertRaises(ContainerDoesNotExist):
            db.get_container(id=1, name='thing 2')
        with self.assertRaises(ContainerDoesNotExist):
            db.get_container()

    def test_get_containers(self):
        db = Database(':memory:')
        self.assertEqual(db.get_containers(), [])
        with db._connection as conn:
            conn.execute(
                'INSERT INTO containers VALUES(1,"thing 1",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(2,"thing 2",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(3,"thing 3",1,"now","now")')
        thing1 = {'container_id': 1, 'name': 'thing 1', 'parent_id': None,
                  'created': 'now', 'modified': 'now'}
        thing2 = {'container_id': 2, 'name': 'thing 2', 'parent_id': None,
                  'created': 'now', 'modified': 'now'}
        thing3 = {'container_id': 3, 'name': 'thing 3', 'parent_id': 1,
                  'created': 'now', 'modified': 'now'}
        self.assertEqual(db.get_containers(), [thing1, thing2, thing3])

    def test_add_note(self):
        db = Database(':memory:')
        with db._connection as conn:
            conn.execute(
                'INSERT INTO containers VALUES(1,"thing 1",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(2,"thing 2",NULL,"now","now")')
        self.assertEqual(db.add_note('note 1', container_id=1), 1)
        self.assertEqual(db.add_note('note 2', container_id=1), 2)
        self.assertEqual(db.add_note('note 3', container_id=2), 3)
        self.assertEqual(db.add_note('note 4', container_name='thing 1'), 4)
        with self.assertRaises(ContainerDoesNotExist):
            db.add_note('note 999')
        with self.assertRaises(ContainerDoesNotExist):
            db.add_note('note 999', container_id=999)
        with self.assertRaises(ContainerDoesNotExist):
            db.add_note('note 5', container_id=1, container_name='thing 2')
        with db._connection as conn:
            result = conn.execute(
                'SELECT note_id, name, container_id FROM notes')
        self.assertEqual([dict(r) for r in result.fetchall()],
                         [{'note_id': 1, 'name': 'note 1', 'container_id': 1},
                          {'note_id': 2, 'name': 'note 2', 'container_id': 1},
                          {'note_id': 3, 'name': 'note 3', 'container_id': 2},
                          {'note_id': 4, 'name': 'note 4', 'container_id': 1}])

    def test_get_note(self):
        db = Database(':memory:')
        with db._connection as conn:
            conn.execute(
                'INSERT INTO containers VALUES(1,"thing 1",NULL,"now","now")')
            conn.execute(
                'INSERT INTO notes VALUES(1,"note 1",NULL,1,"now","now")')
            conn.execute(
                'INSERT INTO notes VALUES(2,"note 2",NULL,1,"now","now")')
        note1 = {'note_id': 1, 'name': 'note 1', 'container_id': 1,
                 'description': None, 'created': 'now', 'modified': 'now'}
        note2 = {'note_id': 2, 'name': 'note 2', 'container_id': 1,
                 'description': None, 'created': 'now', 'modified': 'now'}
        self.assertEqual(db.get_note(id=1), note1)
        self.assertEqual(db.get_note(id=2), note2)
        with self.assertRaises(NoteDoesNotExist):
            db.get_note(id=99)
        with self.assertRaises(NoteDoesNotExist):
            db.get_note()

    def test_get_notes(self):
        db = Database(':memory:')
        self.assertEqual(db.get_notes(), [])
        with db._connection as conn:
            conn.execute(
                'INSERT INTO containers VALUES(1,"thing 1",NULL,"now","now")')
            conn.execute(
                'INSERT INTO containers VALUES(2,"thing 2",NULL,"now","now")')
            conn.execute(
                'INSERT INTO notes VALUES(1,"note 1",NULL,1,"now","now")')
            conn.execute(
                'INSERT INTO notes VALUES(2,"note 2",NULL,1,"now","now")')
            conn.execute(
                'INSERT INTO notes VALUES(3,"note 3",NULL,2,"now","now")')
        note1 = {'note_id': 1, 'name': 'note 1', 'container_id': 1,
                 'description': None, 'created': 'now', 'modified': 'now'}
        note2 = {'note_id': 2, 'name': 'note 2', 'container_id': 1,
                 'description': None, 'created': 'now', 'modified': 'now'}
        note3 = {'note_id': 3, 'name': 'note 3', 'container_id': 2,
                 'description': None, 'created': 'now', 'modified': 'now'}
        self.assertEqual(db.get_notes(), [note1, note2, note3])
        self.assertEqual(db.get_notes(container_id=1), [note1, note2])
        self.assertEqual(db.get_notes(container_name='thing 2'), [note3])

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


class TestContainerDoesNotExist(unittest.TestCase):
    def test_empty(self):
        with self.assertRaises(ContainerDoesNotExist) as cm:
            raise ContainerDoesNotExist()
        self.assertEqual(str(cm.exception), 'Container does not exist.')
        self.assertEqual(cm.exception.message, 'Container does not exist.')
        self.assertEqual(cm.exception.id, None)
        self.assertEqual(cm.exception.name, None)

    def test_id(self):
        with self.assertRaises(ContainerDoesNotExist) as cm:
            raise ContainerDoesNotExist(id=1)
        self.assertEqual(str(cm.exception),
                         'Container with (id=1) does not exist.')
        self.assertEqual(cm.exception.message,
                         'Container with (id=1) does not exist.')
        self.assertEqual(cm.exception.id, 1)
        self.assertEqual(cm.exception.name, None)

    def test_name(self):
        with self.assertRaises(ContainerDoesNotExist) as cm:
            raise ContainerDoesNotExist(name='bad')
        self.assertEqual(str(cm.exception),
                         'Container with (name="bad") does not exist.')
        self.assertEqual(cm.exception.message,
                         'Container with (name="bad") does not exist.')
        self.assertEqual(cm.exception.id, None)
        self.assertEqual(cm.exception.name, 'bad')

    def test__name_and_id(self):
        with self.assertRaises(ContainerDoesNotExist) as cm:
            raise ContainerDoesNotExist(id=1, name='bad')
        self.assertEqual(str(cm.exception),
                         'Container with (id=1,name="bad") does not exist.')
        self.assertEqual(cm.exception.message,
                         'Container with (id=1,name="bad") does not exist.')
        self.assertEqual(cm.exception.id, 1)
        self.assertEqual(cm.exception.name, 'bad')


class TestContainerNameExists(unittest.TestCase):
    def test_empty(self):
        with self.assertRaises(TypeError) as cm:
            raise ContainerNameExists()

    def test_name(self):
        with self.assertRaises(ContainerNameExists) as cm:
            raise ContainerNameExists('bad')
        self.assertEqual(
            str(cm.exception),
            'Cannot add container named "bad", it already exists.')


class TestNoteDoesNotExist(unittest.TestCase):
    def test_empty(self):
        with self.assertRaises(NoteDoesNotExist) as cm:
            raise NoteDoesNotExist()
        self.assertEqual(str(cm.exception), 'Note does not exist.')
        self.assertEqual(cm.exception.message, 'Note does not exist.')
        self.assertEqual(cm.exception.id, None)
        self.assertEqual(cm.exception.name, None)

    def test_id(self):
        with self.assertRaises(NoteDoesNotExist) as cm:
            raise NoteDoesNotExist(id=1)
        self.assertEqual(str(cm.exception), 'Note with (id=1) does not exist.')
        self.assertEqual(cm.exception.message,
                         'Note with (id=1) does not exist.')
        self.assertEqual(cm.exception.id, 1)
        self.assertEqual(cm.exception.name, None)

    def test_name(self):
        with self.assertRaises(NoteDoesNotExist) as cm:
            raise NoteDoesNotExist(name='bad')
        self.assertEqual(str(cm.exception),
                         'Note with (name="bad") does not exist.')
        self.assertEqual(cm.exception.message,
                         'Note with (name="bad") does not exist.')
        self.assertEqual(cm.exception.id, None)
        self.assertEqual(cm.exception.name, 'bad')

    def test__name_and_id(self):
        with self.assertRaises(NoteDoesNotExist) as cm:
            raise NoteDoesNotExist(id=1, name='bad')
        self.assertEqual(str(cm.exception),
                         'Note with (id=1,name="bad") does not exist.')
        self.assertEqual(cm.exception.message,
                         'Note with (id=1,name="bad") does not exist.')
        self.assertEqual(cm.exception.id, 1)
        self.assertEqual(cm.exception.name, 'bad')
