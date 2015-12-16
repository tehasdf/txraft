# coding: utf-8

from collections import namedtuple
import random

from twisted.python.constants import Names, NamedConstant
from twisted.internet import reactor
from twisted.internet.defer import succeed, inlineCallbacks, returnValue, gatherResults, DeferredList
from twisted.protocols.amp import AMP, CommandLocator, Command
from twisted.internet.task import LoopingCall


Entry = namedtuple('Entry', ['term', 'payload'])


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

        self._appendEntriesLocks = {}

    def _getElectionTimeout(self):
        if self._electionTimeout is None:
            return random.uniform(0.15, 0.3)
        else:
            return self._electionTimeout

    @inlineCallbacks
    def _callAppendEntries(self, targetId, entries):
        currentTerm, prevLogIndex, prevLogTerm = yield gatherResults([
            self._store.getCurrentTerm(),
            self._store.getLastIndex(),
            self._store.getLastTerm(),
        ])

        while True:
            peerTerm, result = yield self._rpc.appendEntries(targetId, currentTerm, self.id,
                prevLogIndex, prevLogTerm, entries, self.commitIndex)
            if not result:
                if peerTerm > currentTerm:
                    raise Exception("term - turn to follower?")
                if entries:
                    firstIndex = min(entries.keys())
                    previous = firstIndex - 1
                else:
                    previous = yield self._store.getLastIndex()
                previousEntry = yield self._store.getByIndex(previous)
                try:
                    previous_previousEntry = yield self._store.getByIndex(previous - 1)
                except KeyError:
                    prevLogIndex = 0
                    prevLogTerm = 0
                else:
                    prevLogTerm = previous_previousEntry.term
                    prevLogIndex = previous - 1
                entries[previous] = previousEntry
            else:
                break
        returnValue(1)

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
        self.nextIndex = {}
        self.matchIndex = {}
        self._appendEntriesLocks = {}
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
                last_index = max(entries.keys())
                commitIndex = min([leaderCommit, last_index])
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

    def appendEntries(self, otherId, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit):
        target = [node for node in self.nodes if node.id == otherId][0]
        return target.respond_appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
