from twisted.trial.unittest import TestCase

from . import MockStoreDontUse, Entry


class TestMockStoreInsert(TestCase):
    def test_empty(self):
        store = MockStoreDontUse([])
        newentry = Entry(term=1, index=2, payload=True)
        store.insert([newentry])

        self.assertEqual(store.log, [newentry])

    def test_noconflict(self):
        oldentry = Entry(term=1, index=1, payload=True)
        store = MockStoreDontUse([oldentry])
        newentry = Entry(term=1, index=2, payload=True)
        store.insert([newentry])

        self.assertEqual(store.log, [oldentry, newentry])

    def test_conflict_last(self):
        oldentry = Entry(term=1, index=1, payload=False)
        store = MockStoreDontUse([oldentry])
        newentry = Entry(term=2, index=1, payload=True)
        store.insert([newentry])

        self.assertEqual(store.log, [newentry])

    def test_conflict_many(self):

        oldentry1 = Entry(term=1, index=1, payload=1)
        oldentry2 = Entry(term=1, index=2, payload=2)
        oldentry3 = Entry(term=1, index=3, payload=3)
        store = MockStoreDontUse([oldentry1, oldentry2, oldentry3])
        newentry1 = Entry(term=2, index=2, payload=4)
        newentry2 = Entry(term=2, index=3, payload=5)
        newentry3 = Entry(term=2, index=4, payload=6)
        store.insert([newentry1, newentry2, newentry3])

        self.assertEqual(store.log, [oldentry1, newentry1, newentry2, newentry3])
