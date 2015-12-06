from collections import namedtuple
from twisted.python.constants import Names, NamedConstant
from twisted.internet import reactor
from twisted.internet.defer import succeed, inlineCallbacks, returnValue
from twisted.protocols.amp import AMP, CommandLocator, Command


Entry = namedtuple('Entry', ['term', 'index', 'payload'])


class STATE(Names):
    FOLLOWER = NamedConstant()
    CANDIDATE = NamedConstant()
    LEADER = NamedConstant()


class RaftNode(object):
    """
    """
    def __init__(self, store, rpc, clock=reactor):
        self._store = store
        self._rpc = rpc

        self._state = STATE.FOLLOWER

        self.commitIndex = 0
        self.lastApplied = 0

    @inlineCallbacks
    def respond_appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        currentTerm = yield self.store.getCurrentTerm()

        if term < currentTerm:
            returnValue((currentTerm, False))

        if not (yield self.store.contains(term=prevLogTerm, index=prevLogIndex)):
            returnValue((currentTerm, False))

        yield self.store.insert(entries)

        returnValue((currentTerm, True))


    def respond_requestVote(self, term, canditateId, lastLogIndex, lastLogTerm):
        pass


class MockStoreDontUse(object):
    def __init__(self, entries=None):
        self.currentTerm = 0
        self.votedFor = None
        if entries is None:
            entries = []
        self.log = entries

    def addToLog(self, entry):
        self.log.append(entry)
        currentIndex = len(self.log) + 1
        return succeed(currentIndex)

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
        return any(e.term == term and e.index == index for e in self.log)

    def insert(self, entries):
        for entry in entries:
            self.log.append(entry)

class MockRPC(object):
    def __init__(self, nodes=None):
        if nodes is None:
            self.nodes = []
        self.nodes = nodes

    def simpleAddNode(self, node):
        self.nodes.append(node)


