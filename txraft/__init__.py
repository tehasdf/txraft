# coding: utf-8

from collections import namedtuple
import random

from twisted.python.constants import Names, NamedConstant
from twisted.internet import reactor
from twisted.internet.defer import succeed, inlineCallbacks, returnValue, gatherResults, DeferredList
from twisted.protocols.amp import AMP, CommandLocator, Command
from twisted.internet.task import LoopingCall


Entry = namedtuple('Entry', ['term', 'index', 'payload'])


class STATE(Names):
    FOLLOWER = NamedConstant()
    CANDIDATE = NamedConstant()
    LEADER = NamedConstant()


class RaftNode(object):
    """
    """

    def __init__(self, node_id, store, rpc, clock=reactor, electionTimeout=None):
        self.id = node_id
        self._store = store
        self._rpc = rpc
        self._electionTimeout = None

        self._state = STATE.FOLLOWER

        self.commitIndex = 0
        self.lastApplied = 0

        self.electionTimeoutLoop = LoopingCall(self._onElectionTimeout)
        self.electionTimeoutLoop.clock = clock
        self.electionTimeoutLoop.start(self._getElectionTimeout(), now=False)

        self.heartbeatSender = LoopingCall(self._sendHeartbeat)
        self.heartbeatSender.clock = clock

    def _getElectionTimeout(self):
        if self._electionTimeout is None:
            return random.uniform(0.15, 0.3)
        else:
            return self._electionTimeout

    def _sendHeartbeat(self):
        pass

    @inlineCallbacks
    def _onElectionTimeout(self):
        self._state = STATE.CANDIDATE
        self.electionTimeoutLoop.stop()
        currentTerm, logIndex, term = yield gatherResults([
                self._store.getCurrentTerm(),
                self._store.getLastIndex(),
                self._store.getLastTerm(),
            ])
        yield self._store.setCurrentTerm(currentTerm + 1)

        electionResult = yield self._rpc.requestVotes(currentTerm, self.id, logIndex, term)
        if electionResult:
            assert self._state is STATE.CANDIDATE
            self.becomeLeader()

    def becomeLeader(self):
        self._state = STATE.LEADER
        self.heartbeatSender.start(0.05, now=True)

    @inlineCallbacks
    def respond_appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        currentTerm = yield self._store.getCurrentTerm()

        if term < currentTerm:
            returnValue((currentTerm, False))

        if term > currentTerm:
            yield self._store.setCurrentTerm(term)
            if self._state is not STATE.FOLLOWER:
                self._state = STATE.FOLLOWER

        if not (yield self._store.contains(term=prevLogTerm, index=prevLogIndex)):
            returnValue((currentTerm, False))

        yield self._store.insert(entries)

        if leaderCommit > self.commitIndex:
            if entries:
                commitIndex = min([leaderCommit, entries[-1].index])
            else:
                commitIndex = leaderCommit

        returnValue((currentTerm, True))

    @inlineCallbacks
    def respond_requestVote(self, term, canditateId, lastLogIndex, lastLogTerm):
        currentTerm = yield self._store.getCurrentTerm()

        if term < currentTerm:
            returnValue((currentTerm, False))

        if term > currentTerm:
            yield self._store.setCurrentTerm(term)
            if self._state is not STATE.FOLLOWER:
                self._state = STATE.FOLLOWER

        votedFor = yield self._store.getVotedFor()
        if votedFor is None or votedFor == canditateId:
            currentTerm, logIndex = yield gatherResults([
                self._store.getCurrentTerm(), # XXX should this be the current term, or term of last log entry?
                self._store.getLastIndex()
            ])
            if (currentTerm > lastLogTerm) or (currentTerm == lastLogTerm and logIndex > lastLogIndex):
                returnValue((term, False))
            else:
                yield self._store.setVotedFor(canditateId)
                returnValue((term, True))
        returnValue((term, False))


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

    def getLastIndex(self):
        if not self.log:
            return succeed(0)
        return succeed(self.log[-1].index)

    def getLastTerm(self):
        if not self.log:
            return succeed(0)
        return succeed(self.log[-1].term)

    def contains(self, term, index):
        if not self.log and term == index == 0:
            return True
        return any(e.term == term and e.index == index for e in self.log)

    def _byIndex(self, ix):
        for entry in self.log:
            if entry.index == ix:
                return entry
        raise IndexError(ix)

    def deleteAfter(self, ix, inclusive=True):
        deleteFromIndex = self._byIndex(ix).index
        if not inclusive:
            deleteFromIndex -= 1
        self.log = [e for e in self.log if e.index < deleteFromIndex]

    def insert(self, entries):
        for entry in entries:
            try:
                oldentry = self._byIndex(entry.index)
            except IndexError:
                pass
            else:
                if oldentry.term != entry.term:
                    self.deleteAfter(entry.index)

        for entry in entries:
            self.log.append(entry)


class MockRPC(object):
    def __init__(self, nodes=None):
        if nodes is None:
            nodes = []
        self.nodes = nodes

    def simpleAddNode(self, node):
        self.nodes.append(node)

    @inlineCallbacks
    def requestVotes(self, term, canditateId, lastLogIndex, lastLogTerm):
        responses = [
            node.respond_requestVote(term, canditateId, lastLogIndex, lastLogTerm)
            for node in self.nodes if node.id != canditateId
        ]

        votesYes = 1
        votesNo = 0
        while True:
            (responder_term, result), ix = yield DeferredList(responses, fireOnOneCallback=True)
            if result:
                votesYes += 1
            else:
                votesNo += 1

            if votesYes * 2 > len(self.nodes):
                returnValue(True)
                break
            if votesNo * 2 > len(self.nodes):
                returnValue(False)

            responses.pop(ix)
            if not responses:
                returnValue(False)

