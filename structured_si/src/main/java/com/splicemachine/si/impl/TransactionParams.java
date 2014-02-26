package com.splicemachine.si.impl;

/**
 * Package up several transaction parameters so they can be passed around as a single object.
 */
class TransactionParams {
    final TransactionId parent;
    final boolean dependent;
    final boolean allowWrites;
    final boolean additive;
    final Boolean readUncommitted;
    final Boolean readCommitted;
		final byte[] writeTable;

		TransactionParams(TransactionId parent,
											boolean dependent,
											boolean allowWrites,
											boolean additive,
											Boolean readUncommitted,
											Boolean readCommitted) {
			this(parent,dependent,allowWrites,additive,readUncommitted,readCommitted,null);
		}

    TransactionParams(TransactionId parent,
											boolean dependent,
											boolean allowWrites,
											boolean additive,
											Boolean readUncommitted,
											Boolean readCommitted,
											byte[] writeTable) {
        this.parent = parent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.additive = additive;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
				this.writeTable = writeTable;
    }
}
