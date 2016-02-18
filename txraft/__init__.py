# coding: utf-8

from collections import namedtuple
import random

from twisted.python.constants import Names, NamedConstant
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, gatherResults, DeferredList
from twisted.protocols.amp import AMP, CommandLocator, Command
from twisted.internet.task import LoopingCall

from txraft.commands import AppendEntriesCommand, RequestVotesCommand

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
            command = AppendEntriesCommand(term=currentTerm,
                leaderId=self.id,
                prevLogIndex=prevLogIndex,
                prevLogTerm=prevLogTerm,
                entries=entries,
                leaderCommit=self.commitIndex)

            peerTerm, result = yield self._rpc.appendEntries(targetId, command)
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

        command = RequestVotesCommand(term=currentTerm,
            candidateId=self.id,
            lastLogIndex=logIndex,
            lastLogTerm=term)

        electionResult = yield self._rpc.requestVotes(command)
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
    def respond_appendEntries(self, command):
        term = command.term
        currentTerm = yield self._store.getCurrentTerm()
        if term < currentTerm:
            returnValue((currentTerm, False))

        if term > currentTerm:
            yield self._store.setCurrentTerm(term)
            if self._state is not STATE.FOLLOWER:
                self._state = STATE.FOLLOWER

        if not (yield self._store.contains(term=command.prevLogTerm,
                                           index=command.prevLogIndex)):
            returnValue((currentTerm, False))

        yield self._store.insert(command.entries)

        if command.leaderCommit > self.commitIndex:
            if command.entries:
                last_index = max(command.entries.keys())
                commitIndex = min([command.leaderCommit, last_index])
            else:
                commitIndex = command.leaderCommit

        returnValue((currentTerm, True))

    @inlineCallbacks
    def respond_requestVote(self, command):
        term = command.term
        lastLogTerm = command.lastLogTerm
        lastLogIndex = command.lastLogIndex

        currentTerm = yield self._store.getCurrentTerm()

        if term < currentTerm:
           returnValue((currentTerm, False))

        if term > currentTerm:
            yield self._store.setCurrentTerm(term)
            if self._state is not STATE.FOLLOWER:
                self._state = STATE.FOLLOWER

        votedFor = yield self._store.getVotedFor()
        if votedFor is None or votedFor == command.candidateId:
            currentTerm, logIndex = yield gatherResults([
                self._store.getCurrentTerm(), # XXX should this be the current term, or term of last log entry?
                self._store.getLastIndex()
            ])

            if (currentTerm > lastLogTerm) or (currentTerm == lastLogTerm
                                               and logIndex > lastLogIndex):
                returnValue((term, False))
            else:
                yield self._store.setVotedFor(command.candidateId)
                returnValue((term, True))
        returnValue((term, False))




class MockRPC(object):
    def __init__(self, nodes=None):
        if nodes is None:
            nodes = []
        self.nodes = nodes

    def simpleAddNode(self, node):
        self.nodes.append(node)

    @inlineCallbacks
    def requestVotes(self, command):
        responses = [
            node.respond_requestVote(command)
            for node in self.nodes
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

    def appendEntries(self, otherId, command):
        target = [node for node in self.nodes if node.id == otherId][0]
        return target.respond_appendEntries(command)
