from twisted.internet.defer import succeed
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from txraft import  Entry, RaftNode, MockRPC, STATE
from txraft.commands import AppendEntriesCommand, RequestVotesCommand

class MockStoreDontUse(object):
    def __init__(self, entries=None):
        self.currentTerm = 0
        self.votedFor = None
        if entries is None:
            entries = {}
        self.log = entries

    def getLastIndex(self):
        if not self.log:
            return succeed(0)

        return succeed(max(self.log.iterkeys()))

    def getLastTerm(self):
        if not self.log:
            return succeed(0)

        return (self.getLastIndex()
            .addCallback(lambda index: self.log[index].term)
        )

    def getByIndex(self, ix):
        return succeed(self.log[ix])

    def setVotedFor(self, votedFor):
        self.votedFor = votedFor
        return succeed(True)

    def setCurrentTerm(self, currentTerm):
        self.currentTerm = currentTerm
        return succeed(True)

    def getVotedFor(self):
        return succeed(self.votedFor)

    def getCurrentTerm(self):
        return succeed(self.currentTerm)

    def contains(self, term, index):
        if term == index == 0:
            return True
        return index in self.log and self.log[index].term == term

    def deleteAfter(self, ix, inclusive=True):
        if not inclusive:
            ix += 1
        while True:
            if not ix in self.log:
                break
            del self.log[ix]
            ix += 1

    def insert(self, entries):
        for index, entry in entries.iteritems():
            if index in self.log and self.log[index].term != entry.term:
                self.deleteAfter(index)

        for index, entry in entries.iteritems():
            self.log[index] = entry


class TestMockStoreInsert(TestCase):
    def test_empty(self):
        store = MockStoreDontUse()
        newentry = Entry(term=1, payload=True)
        store.insert({1: newentry})

        self.assertEqual(store.log, {1: newentry})

    def test_noconflict(self):
        oldentry = Entry(term=1, payload=True)
        store = MockStoreDontUse({1: oldentry})
        newentry = Entry(term=1, payload=True)
        store.insert({2: newentry})

        self.assertEqual(store.log, {1: oldentry, 2: newentry})

    def test_conflict_last(self):
        oldentry = Entry(term=1, payload=False)
        store = MockStoreDontUse({1: oldentry})
        newentry = Entry(term=2, payload=True)
        store.insert({1: newentry})

        self.assertEqual(store.log, {1: newentry})

    def test_conflict_many(self):

        oldentry1 = Entry(term=1, payload=1)
        oldentry2 = Entry(term=1, payload=2)
        oldentry3 = Entry(term=1, payload=3)
        store = MockStoreDontUse({1: oldentry1, 2: oldentry2, 3: oldentry3})
        newentry1 = Entry(term=2, payload=4)
        newentry2 = Entry(term=2, payload=5)
        newentry3 = Entry(term=2, payload=6)
        store.insert({2: newentry1, 3: newentry2, 4: newentry3})

        self.assertEqual(store.log, {1: oldentry1, 2: newentry1, 3: newentry2, 4: newentry3})


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

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId=2,
            lastLogIndex=4,
            lastLogTerm=4))

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

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId=2,
            lastLogIndex=4,
            lastLogTerm=4))
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId=3,
            lastLogIndex=4,
            lastLogTerm=4))
        term, result = self.successResultOf(resp)
        self.assertTrue(result)

    def test_respond_requestVote_lowerTerm(self):
        store = MockStoreDontUse()
        store.setCurrentTerm(3)
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(RequestVotesCommand(term=2,
            candidateId='id',
            lastLogIndex=4,
            lastLogTerm=4))
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

    def test_respond_requestVote_oldLog(self):
        store = MockStoreDontUse(entries={
            2: Entry(term=2, payload=1),
            3: Entry(term=3, payload=2)
        })
        store.setCurrentTerm(3)
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId='id',
            lastLogIndex=2,
            lastLogTerm=2))
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId='id',
            lastLogIndex=4,
            lastLogTerm=2))
        term, result = self.successResultOf(resp)
        self.assertFalse(result)

        resp = node.respond_requestVote(RequestVotesCommand(term=4,
            candidateId='id',
            lastLogIndex=2,
            lastLogTerm=3))
        term, result = self.successResultOf(resp)
        self.assertFalse(result)


class TestAppendEntries(TestCase):
    def test_respond_appendEntries_simple(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        newentry = Entry(term=0, payload=1)

        resp = node.respond_appendEntries(AppendEntriesCommand(term=0,
            leaderId=2,
            prevLogIndex=0,
            prevLogTerm=0,
            entries={1: newentry},
            leaderCommit=1))

        term, result = self.successResultOf(resp)
        self.assertEqual(term, 0)
        self.assertTrue(result)
        self.assertEqual(store.log, {1: newentry})

    def test_respond_appendEntries_empty(self):
        store = MockStoreDontUse()
        rpc = MockRPC()
        clock = Clock()
        node = RaftNode(1, store, rpc, clock=clock)

        newentry = Entry(term=0, payload=1)

        resp = node.respond_appendEntries(AppendEntriesCommand(term=0,
            leaderId=2,
            prevLogIndex=0,
            prevLogTerm=0,
            entries={},
            leaderCommit=1))

        term, result = self.successResultOf(resp)
        self.assertEqual(term, 0)
        self.assertTrue(result)


class TestCallingAppendEntries(TestCase):
    def test_backwards(self):
        clock = Clock()

        leader_store = MockStoreDontUse(entries={
            1: Entry(term=1, payload=1),
            2: Entry(term=2, payload=2),
        })
        leader_store.setCurrentTerm(2)
        leader_rpc = MockRPC()
        leader = RaftNode(1, leader_store, leader_rpc, clock=clock)

        follower_store = MockStoreDontUse()
        follower_rpc = MockRPC()
        follower = RaftNode(2, follower_store, follower_rpc, clock=clock)

        leader_rpc.simpleAddNode(follower)
        follower_rpc.simpleAddNode(leader)

        d = leader._callAppendEntries(follower.id, {})
        res = self.successResultOf(d)

        self.assertEqual(leader_store.log, follower_store.log)

    def test_add(self):
        clock = Clock()

        leader_store = MockStoreDontUse(entries={
            1: Entry(term=1, payload=1),
            2: Entry(term=2, payload=2),
            3: Entry(term=2, payload=3),
        })
        leader_store.setCurrentTerm(2)
        leader_rpc = MockRPC()
        leader = RaftNode(1, leader_store, leader_rpc, clock=clock)

        follower_store = MockStoreDontUse({
            1: Entry(term=1, payload=1)
        })
        follower_rpc = MockRPC()
        follower = RaftNode(2, follower_store, follower_rpc, clock=clock)

        leader_rpc.simpleAddNode(follower)
        follower_rpc.simpleAddNode(leader)

        d = leader._callAppendEntries(follower.id, {})
        res = self.successResultOf(d)

        self.assertEqual(leader_store.log, follower_store.log)


    def test_remove_incorrect(self):
        clock = Clock()

        leader_store = MockStoreDontUse(entries={
            1: Entry(term=1, payload=1),
            2: Entry(term=2, payload=2),
            3: Entry(term=2, payload=3),
        })
        leader_store.setCurrentTerm(2)
        leader_rpc = MockRPC()
        leader = RaftNode(1, leader_store, leader_rpc, clock=clock)

        follower_store = MockStoreDontUse({
            1: Entry(term=1, payload=1),
            2: Entry(term=5, payload=1)
        })
        follower_rpc = MockRPC()
        follower = RaftNode(2, follower_store, follower_rpc, clock=clock)

        leader_rpc.simpleAddNode(follower)
        follower_rpc.simpleAddNode(leader)

        d = leader._callAppendEntries(follower.id, {})
        res = self.successResultOf(d)

        self.assertEqual(leader_store.log, follower_store.log)


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

        # for node, rpc, store, clock in nodes:
        #     print 'asd', node._state

