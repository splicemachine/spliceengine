package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.relations.api.TupleHandler;

public class TransactionSchema {
	final String relationIdentifier;
	final Object siFamily;
	final Object startQualifier;
	final Object commitQualifier;
	final Object statusQualifier;

	public TransactionSchema(String relationIdentifier, Object siFamily, Object startQualifier, Object commitQualifier,
							 Object statusQualifier) {
		this.relationIdentifier = relationIdentifier;
		this.siFamily = siFamily;
		this.startQualifier = startQualifier;
		this.commitQualifier = commitQualifier;
		this.statusQualifier = statusQualifier;
	}

	public TransactionSchema encodedSchema(TupleHandler tupleHandler) {
		return new TransactionSchema(relationIdentifier,
				tupleHandler.makeFamily((String) siFamily),
				tupleHandler.makeQualifier(startQualifier),
				tupleHandler.makeQualifier(commitQualifier),
				tupleHandler.makeQualifier(statusQualifier));
	}
}
