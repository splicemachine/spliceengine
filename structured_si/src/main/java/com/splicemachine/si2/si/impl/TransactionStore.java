package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Iterator;

public class TransactionStore {
	private final TupleHandler handler;
	private final RelationReader reader;
	private final RelationWriter writer;

	private final TransactionSchema transactionSchema;
	private final TransactionSchema encodedSchema;

	public TransactionStore(TransactionSchema transactionSchema, TupleHandler handler,
							RelationReader reader, RelationWriter writer) {
		this.transactionSchema = transactionSchema;
		this.encodedSchema = transactionSchema.encodedSchema(handler);
		this.handler = handler;
		this.reader = reader;
		this.writer = writer;
	}

	public void recordNewTransaction(SiTransactionId startTransactionTimestamp, TransactionStatus status) {
		writeTuple(makeCreateTuple(startTransactionTimestamp, status));
	}

	public void recordTransactionCommit(SiTransactionId startTransactionTimestamp, long commitTransactionTimestamp, TransactionStatus newStatus) {
		writeTuple(makeCommitTuple(startTransactionTimestamp, commitTransactionTimestamp, newStatus));
	}

	public void recordTransactionStatusChange(SiTransactionId startTransactionTimestamp, TransactionStatus newStatus) {
		writeTuple(makeStatusUpdateTuple(startTransactionTimestamp, newStatus));
	}

	public Object[] getTransactionStatus(SiTransactionId transactionId) {
		Object tupleKey = handler.makeTupleKey(new Object[]{transactionIdToRowKey(transactionId)});

		Relation transactionRelation = reader.open(transactionSchema.relationIdentifier);
		try {
			TupleGet get = handler.makeTupleGet(tupleKey, tupleKey, null, null, null);
			final Iterator results = reader.read(transactionRelation, get);
			if( results.hasNext() ) {
				Object resultTuple = results.next();
				assert (!results.hasNext());
				final Object value = handler.getLatestCellForColumn(resultTuple, encodedSchema.siFamily, encodedSchema.statusQualifier);
				TransactionStatus status = TransactionStatus.values()[((Integer) handler.fromValue(value, Integer.class))];
				final Object commitValue = handler.getLatestCellForColumn(resultTuple, encodedSchema.siFamily, encodedSchema.commitQualifier);
				Long commitTimestamp = null;
				if (commitValue != null) {
					commitTimestamp = (Long) handler.fromValue(commitValue, Long.class);
				}
				return new Object[] {status, commitTimestamp};
			}
		} finally {
   			reader.close(transactionRelation);
		}
		throw new RuntimeException("transaction ID not found");
	}

	private Object makeStatusUpdateTuple(SiTransactionId transactionId, TransactionStatus newStatus) {
		Object tuple = makeBaseTuple(transactionId);
		addFieldToTuple(tuple, encodedSchema.statusQualifier, newStatus.ordinal());
		return tuple;
	}

	private Object makeCreateTuple(SiTransactionId transactionId, TransactionStatus status) {
		Object tuple = makeBaseTuple(transactionId);
		addFieldToTuple(tuple, encodedSchema.startQualifier, transactionId.id);
		addFieldToTuple(tuple, encodedSchema.statusQualifier, status.ordinal());
		return tuple;
	}

	private Object makeCommitTuple(SiTransactionId transactionId, long commitTransactionTimestamp, TransactionStatus newStatus) {
		Object tuple = makeBaseTuple(transactionId);
		addFieldToTuple(tuple, encodedSchema.commitQualifier, commitTransactionTimestamp);
		addFieldToTuple(tuple, encodedSchema.statusQualifier, newStatus.ordinal());
		return tuple;
	}

	private Object makeBaseTuple(SiTransactionId transactionId) {
		Object tupleKey = handler.makeTupleKey(new Object[]{transactionIdToRowKey(transactionId)});
		return handler.makeTuplePut(tupleKey, null);
	}

	private long transactionIdToRowKey(SiTransactionId transactionId) {
		byte[] result = Bytes.toBytes(transactionId.id);
		ArrayUtils.reverse(result);
		return Bytes.toLong(result);
	}

	private void addFieldToTuple(Object tuple, Object qualifier, Object value) {
		handler.addCellToTuple(tuple, encodedSchema.siFamily, qualifier, null, handler.makeValue(value));
	}

	private void writeTuple(Object tuple) {
		final Relation transactionRelation = reader.open(transactionSchema.relationIdentifier);
		try {
			writer.write(transactionRelation, Arrays.asList(tuple));
		} finally {
			reader.close(transactionRelation);
		}
	}

}
