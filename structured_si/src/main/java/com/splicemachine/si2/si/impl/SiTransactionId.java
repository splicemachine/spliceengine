package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.si.api.TransactionId;

public class SiTransactionId implements TransactionId {
	final long id;

	public SiTransactionId(long id) {
		this.id = id;
	}
}
