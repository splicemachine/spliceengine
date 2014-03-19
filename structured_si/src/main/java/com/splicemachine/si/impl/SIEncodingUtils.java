package com.splicemachine.si.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.si.api.TransactionStatus;

/**
 * Utilities around encoding and decoding stuff.
 *
 * @author Scott Fines
 * Date: 2/21/14
 */
public class SIEncodingUtils {

		private SIEncodingUtils(){}

		/**
		 * Read the contents of a Result and produce a Transaction object.
		 *
		 * @param result the result to decode
		 * @return the transaction represented by the result.
		 * @throws java.io.IOException if something goes wrong
		 */
		public static Transaction decodeTransactionResults(long transactionId,
																								Result result,
																								EncodedTransactionSchema schema,
																								TransactionStore transactionStore) throws IOException {
				// TODO: create optimized versions of this code block that only load the data required by the caller, or make the loading lazy
				boolean dependent = Bytes.toBoolean(result.getValue(schema.siFamily,schema.dependentQualifier));
				final TransactionBehavior transactionBehavior = dependent ?
								StubTransactionBehavior.instance :
								IndependentTransactionBehavior.instance;

				byte[] family = schema.siFamily;
				long beginTimestamp = nonNullLong(result.getValue(family, schema.startQualifier));
				long keepAlive = result.getColumnLatestCell(family,schema.keepAliveQualifier).getTimestamp();
				byte[] parentIdBytes = result.getValue(family, schema.parentQualifier);
				Transaction parent = parentIdBytes==null? Transaction.rootTransaction : transactionStore.getTransaction(Bytes.toLong(parentIdBytes));
				boolean allowWrites = notNullBoolean(result.getValue(family,schema.allowWritesQualifier));
				boolean additive = notNullBoolean(result.getValue(family, schema.additiveQualifier));
				Boolean readUncommitted = decode(result.getValue(family, schema.readUncommittedQualifier), Boolean.class);
				Boolean readCommitted = decode(result.getValue(family,schema.readCommittedQualifier),Boolean.class);
				TransactionStatus status = decodeStatus(result.getValue(family,schema.statusQualifier));
				Long commitTimestamp  = decode(result.getValue(family,schema.commitQualifier),Long.class);
				Long globalCommitTimestamp   =decode(result.getValue(family,schema.globalCommitQualifier),Long.class);
				Long counterColumn   =decode(result.getValue(family,schema.counterQualifier),Long.class);
				byte[] writeTable = result.getValue(family,schema.writeTableQualifier);
				return new Transaction(transactionBehavior,
								transactionId,
								beginTimestamp,
								keepAlive,
								parent,
								dependent,
								allowWrites,
								additive,
								readUncommitted,
								readCommitted,
								status,
								commitTimestamp,
								globalCommitTimestamp,
								counterColumn,
								writeTable);
		}

		private static TransactionStatus decodeStatus(byte[] value) {
				assert value !=null :"Transaction detected without a status!";
				/*
				 * This is written this way so that transaction tables which are written
				 * with a 4-byte transaction id(e.g. older installations) can be decoded properly,
				 * while newer installations can use the more efficient 1-byte encoding.
				 */
				if(value.length==4)
						return TransactionStatus.forInt(Bytes.toInt(value));
				else
					return TransactionStatus.forByte(value[0]);
		}

		private static boolean notNullBoolean(byte[] value) {
				assert value !=null : "Unexpected null value";
				return Bytes.toBoolean(value);
		}

		private static long nonNullLong(byte[] value) {
				assert value !=null : "Unexpected null value";
				return Bytes.toLong(value);
		}

		@SuppressWarnings("unchecked")
		private static <T> T decode(byte[] value, Class<T> type) {
				if (value == null) {
						return null;
				}
				if (type.equals(Boolean.class)) {
						return (T)(Boolean) Bytes.toBoolean(value);
				} else if (type.equals(Short.class)) {
						return (T)(Short)Bytes.toShort(value);
				} else if (type.equals(Integer.class)) {
						if(value.length<4) return (T)new Integer(-1);
						return (T)(Integer)Bytes.toInt(value);
				} else if (type.equals(Long.class)) {
						return (T)(Long)Bytes.toLong(value);
				} else if (type.equals(Byte.class)) {
						if (value.length > 0) {
								return (T)(Byte)value[0];
						} else {
								return null;
						}
				} else if (type.equals(String.class)) {
						return (T)Bytes.toString(value);
				}
				throw new RuntimeException("unsupported type conversion: " + type.getName());
		}
}
