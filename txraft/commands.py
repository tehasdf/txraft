from attr import attributes, attr


@attributes
class AppendEntriesCommand(object):
    term = attr()
    leaderId = attr()
    prevLogIndex = attr()
    prevLogTerm = attr()
    entries = attr()
    leaderCommit = attr()


@attributes
class RequestVotesCommand(object):
    term = attr()
    candidateId = attr()
    lastLogIndex = attr()
    lastLogTerm = attr()
