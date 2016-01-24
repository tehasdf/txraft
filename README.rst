txraft
======

I was reading about the raft protocol and wanted to implement it...
someday I'll finish it.


The idea is, the RPC should be pluggable, so the raft implementation can use
any trnsport (HTTP, AMP, etc) you want.
The state machine part should also of course be pluggable :)


Hacking
-------

Just run `py.test`

TODO
----
* the AppendEntries call on the leader side needs to keep track of followers'
  current offset etc.
* simplify the rpc interface
