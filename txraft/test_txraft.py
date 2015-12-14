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

    def test_respond_requestVote(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(5, 2, 4, 4)
        term, result = self.successResultOf(resp)
        self.assertTrue(result)
        votedFor = self.successResultOf(store.getVotedFor())
        self.assertEqual(votedFor, 2)

    def test_respond_requestVote_alreadyVoted(self):
        store = MockStoreDontUse()
        store.setVotedFor(3)
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(5, 2, 4, 4)
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(5, 3, 4, 4)
        term, result = self.successResultOf(resp)
        self.assertTrue(result)

    def test_respond_requestVote_lowerTerm(self):
        store = MockStoreDontUse()
        store.setCurrentTerm(3)
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(2, 'id', 4, 4)
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

    def test_respond_requestVote_oldLog(self):
        store = MockStoreDontUse(entries=[
            Entry(term=2, index=2, payload=1),
            Entry(term=3, index=3, payload=2)
        ])
        store.setCurrentTerm(3)
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(4, 'id', 2, 2)
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(4, 'id', 4, 2)
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(4, 'id', lastLogIndex=2, lastLogTerm=3)
        term, result = self.successResultOf(resp)
        self.assertFalse(result)


class TestAppendEntries(TestCase):
    def test_respond_appendEntries_simple(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        newentry = Entry(term=0, index=1, payload=1)

        resp = node.respond_appendEntries(term=0, leaderId=2, prevLogIndex=0,
            prevLogTerm=0, entries=[newentry], leaderCommit=1)
        term, result = self.successResultOf(resp)
        self.assertEqual(term, 0)
        self.assertTrue(result)
        self.assertEqual(store.log, [newentry])

    def test_respond_appendEntries_empty(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        newentry = Entry(term=0, index=1, payload=1)

        resp = node.respond_appendEntries(term=0, leaderId=2, prevLogIndex=0,
            prevLogTerm=0, entries=[], leaderCommit=1)
        term, result = self.successResultOf(resp)
        self.assertEqual(term, 0)
        self.assertTrue(result)

    def test_respond_appendEntries_empty(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        newentry = Entry(term=0, index=1, payload=1)

        resp = node.respond_appendEntries(term=0, leaderId=2, prevLogIndex=0,
            prevLogTerm=0, entries=[], leaderCommit=1)
        term, result = self.successResultOf(resp)
        self.assertEqual(term, 0)
        self.assertTrue(result)


class TestCluster(TestCase):

    def test_cluster(self):
        nodes = []
        for num in range(5):
            clock = Clock()
            rpc = MockRPC()
            store = MockStoreDontUse()
            node = RaftNode(num, store, rpc, clock=clock, electionTimeout=1)
            nodes.append((node, rpc, store, clock))


        for node1, rpc, _, _ in nodes:
            for node2, _, _, _ in nodes:
                if node1 is node2:
                    continue
                rpc.simpleAddNode(node2)


        for node, rpc, store, clock in nodes:
            clock.advance(1.0)

        for node, rpc, store, clock in nodes:
            print 'asd', node._state

