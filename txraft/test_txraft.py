from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from . import MockStoreDontUse, Entry, RaftNode, MockRPC, STATE


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


class TestElection(TestCase):
    def test_three_up(self):
        store1 = MockStoreDontUse()
        store2 = MockStoreDontUse()
        store3 = MockStoreDontUse()
        rpc1 = MockRPC()
        rpc2 = MockRPC()
        rpc3 = MockRPC()

        clock1 = Clock()
        clock2 = Clock()
        clock3 = Clock()

        node1 = RaftNode(1, store1, rpc1, clock=clock1)
        node2 = RaftNode(2, store2, rpc2, clock=clock2)
        node3 = RaftNode(3, store3, rpc3, clock=clock3)

        for rpc in [rpc1, rpc2, rpc3]:
            for node in [node1, node2, node3]:
                rpc.simpleAddNode(node)


        clock1.advance(0.4)
        self.assertIs(node1._state, STATE.LEADER)
