#Query Planning in SpliceMachine
Or: All My Queries suck and I hate you all


##What the hell *is* a query optimizer anyway?

We all think that we know what the answer to this question is: a query optimizer is something that optimizes queries! Except not really, because we probably don't have a nearly good enough grasp of what we mean when we say a given query is "optimal", or what to do if we think we've found one. So we have to spend some time talking about the different ways a query can run in SpliceMachine.

Nothing is for free. This seems obvious, but you have to start somewhere. Every action taken, and every row moved has a cost to it--latency

