from twisted.trial.unittest import TestCase

from . import MockStoreDontUse, Entry


class TestMockStoreInsert(TestCase):
    def test_empty(self):
        store = MockStoreDontUse([])
        newentry = Entry(term=1, index=2, payload=True)
        store.insert([newentry])

        self.assertEqual(len(store.log), 1)
        self.assertIn(newentry, store.log)

