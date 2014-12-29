package com.splicemachine.si;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.PackedTxnFilter;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.kryo.KryoPool;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 2/17/14
 */
public class TransactorTestUtility {
		boolean useSimple = true;
		StoreSetup storeSetup;
		TestTransactionSetup transactorSetup;
		Transactor transactor;
		TxnLifecycleManager control;
		SDataLib dataLib;
		static KryoPool kryoPool;
		
		static {
		        kryoPool = new KryoPool(SpliceConstants.kryoPoolSize);
		}

		public TransactorTestUtility(boolean useSimple,
																 StoreSetup storeSetup,
																 TestTransactionSetup transactorSetup,
																 Transactor transactor,
																 TxnLifecycleManager control) {
				this.useSimple = useSimple;
				this.storeSetup = storeSetup;
				this.transactorSetup = transactorSetup;
				this.transactor = transactor;
				this.control = control;
				dataLib = storeSetup.getDataLib();
		}

		public void insertAge(Txn txn, String name, Integer age) throws IOException {
				insertAgeDirect(useSimple, transactorSetup, storeSetup, txn, name, age);
		}

		public void insertAgeBatch(Object[]... args) throws IOException {
				insertAgeDirectBatch(useSimple, transactorSetup, storeSetup, args);
		}

		public void insertJob(Txn txn, String name, String job) throws IOException {
				insertJobDirect(useSimple, transactorSetup, storeSetup, txn, name, job);
		}

		public void deleteRow(Txn txn, String name) throws IOException {
				deleteRowDirect(useSimple, transactorSetup, storeSetup, txn, name);
		}

		public String read(Txn txn, String name) throws IOException {
				return readAgeDirect(useSimple, transactorSetup, storeSetup, txn, name);
		}
		
		static void insertAgeDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
				Txn txn, String name, Integer age) throws IOException {
				insertField(useSimple, transactorSetup, storeSetup, txn, name, transactorSetup.agePosition, age);
		}

		static void insertAgeDirectBatch(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
						 Object[] args) throws IOException {
			insertFieldBatch(useSimple, transactorSetup, storeSetup, args, transactorSetup.agePosition);
		}

		static void insertJobDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
				Txn txn, String name, String job) throws IOException {
			insertField(useSimple, transactorSetup, storeSetup, txn, name, transactorSetup.jobPosition, job);
		}


		public String scan(Txn txn, String name) throws IOException {
				final STableReader reader = storeSetup.getReader();
				byte[] key = dataLib.newRowKey(new Object[]{name});
        Scan s = transactorSetup.txnOperationFactory.newScan(txn);
        s.setStartRow(key);
        s.setStopRow(key);
//				Get get = transactorSetup.txnOperationFactory.newGet(txn,key);
				OperationWithAttributes scan = transactorSetup.convertTestTypeScan(s,null);
//				Object scan = makeScan(dataLib, key, null, key);
//				transactorSetup.clientTransactor.initializeOperation(txn, (OperationWithAttributes) scan);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Iterator<Result> results = reader.scan(testSTable, scan);
						if (results.hasNext()) {
								Result rawTuple = results.next();
								Assert.assertTrue(!results.hasNext());
								return readRawTuple(useSimple, transactorSetup, storeSetup, txn, name, dataLib, rawTuple, false,
												true, true);
						} else {
								return "";
						}
				} finally {
						reader.close(testSTable);
				}
		}

		public String scanNoColumns(Txn txn, String name, boolean deleted) throws IOException {
				return scanNoColumnsDirect(useSimple, transactorSetup, storeSetup, txn, name, deleted);
		}

		public String scanAll(Txn txn, String startKey, String stopKey, Integer filterValue) throws IOException {
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{startKey});
				byte[] endKey = dataLib.newRowKey(new Object[]{stopKey});
				Scan scan = transactorSetup.txnOperationFactory.newScan(txn);
				scan.setStartRow(key);
				scan.setStopRow(endKey);
        addPredicateFilter(scan);

				OperationWithAttributes owa = transactorSetup.convertTestTypeScan(scan, null);
				if (!useSimple && filterValue != null) {
//						final Scan scan = (Scan) get;
						SingleColumnValueFilter filter = new SingleColumnValueFilter(transactorSetup.family,
										transactorSetup.ageQualifier,
										CompareFilter.CompareOp.EQUAL,
										new BinaryComparator(dataLib.encode(filterValue)));
						filter.setFilterIfMissing(true);
						scan.setFilter(filter);
				}
//				transactorSetup.clientTransactor.initializeOperation(txn, (OperationWithAttributes) get);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Iterator<Result> results = reader.scan(testSTable, owa);

						StringBuilder result = new StringBuilder();
						while (results.hasNext()) {
								final Result value = results.next();
								final String name = Bytes.toString(value.getRow());
								final String s = readRawTuple(useSimple, transactorSetup, storeSetup, txn, name, dataLib, value, false,
												 true, true);
								result.append(s);
								if (s.length() > 0) {
										result.append("\n");
								}
						}
						return result.toString();
				} finally {
						reader.close(testSTable);
				}
		}


		private static void insertField(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																		Txn txn, String name, int index, Object fieldValue)
						throws IOException {
			final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				Object put = makePut(transactorSetup, txn, name, index, fieldValue, dataLib);
				processPutDirect(useSimple, transactorSetup, storeSetup, reader, (OperationWithAttributes)put);
		}

		private static OperationWithAttributes makePut(TestTransactionSetup transactorSetup,
																	Txn txn, String name, int index,
																	Object fieldValue, SDataLib dataLib) throws IOException {
				byte[] key = dataLib.newRowKey(new Object[]{name});
				Put put = transactorSetup.txnOperationFactory.newPut(txn,key);
				final BitSet bitSet = new BitSet();
				if (fieldValue != null)
					bitSet.set(index);
				final EntryEncoder entryEncoder = EntryEncoder.create(KryoPool.defaultPool(), 2, bitSet, null, null, null);
				try {
					if (index == 0 && fieldValue != null) {
						entryEncoder.getEntryEncoder().encodeNext((Integer) fieldValue);
					} else if (fieldValue != null) {
						entryEncoder.getEntryEncoder().encodeNext((String) fieldValue);
					}
						put.add(transactorSetup.family, SpliceConstants.PACKED_COLUMN_BYTES, txn.getTxnId(), entryEncoder.encode());
				} finally {
					entryEncoder.close();
				}
//				transactorSetup.clientTransactor.initializeOperation(txn, put);
				return transactorSetup.convertTestTypePut(put);
		}

		private static void insertFieldBatch(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																				 Object[] args, int index) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				OperationWithAttributes[] puts = new OperationWithAttributes[args.length];
				int i = 0;
				for (Object subArgs : args) {
						Object[] subArgsArray = (Object[]) subArgs;
						Txn transactionId = (Txn) subArgsArray[0];
						String name = (String) subArgsArray[1];
						Object fieldValue = subArgsArray[2];
						OperationWithAttributes put = makePut(transactorSetup, transactionId, name, index, fieldValue, dataLib);
						puts[i] = put;
						i++;
				}
				processPutDirectBatch(useSimple, transactorSetup, storeSetup, reader, puts);
		}

		static void deleteRowDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																Txn txn, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				Mutation put = transactorSetup.txnOperationFactory.newDelete(txn,key);
				OperationWithAttributes deletePut= transactorSetup.convertTestTypePut((Put)put);
//				OperationWithAttributes deletePut = transactorSetup.clientTransactor.createDeletePut(txn, key);
				processPutDirect(useSimple, transactorSetup, storeSetup, reader, deletePut);
		}

		private static void processPutDirect(boolean useSimple, 
																				 TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
																				 OperationWithAttributes put) throws IOException {
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						if (useSimple) {
										Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
						} else {
								storeSetup.getWriter().write(testSTable, put);
						}
				} finally {
						reader.close(testSTable);
				}
		}

		private static void processPutDirectBatch(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
																							OperationWithAttributes[] puts) throws IOException {
				final Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Object tableToUse = testSTable;
						if (useSimple) {
							OperationWithAttributes[] packedPuts = new OperationWithAttributes[puts.length];
							for (int i = 0; i < puts.length; i++) {
								packedPuts[i] = puts[i];
							}
							puts = packedPuts;
						} else {
								tableToUse = new HbRegion(HStoreSetup.regionMap.get(storeSetup.getPersonTableName()));
						}
						final Object[] results = transactorSetup.transactor.processPutBatch(tableToUse, transactorSetup.rollForwardQueue, puts);
						for (Object r : results) {
								Assert.assertEquals(HConstants.OperationStatusCode.SUCCESS, ((OperationStatus) r).getOperationStatusCode());
						}
				} finally {
						reader.close(testSTable);
				}
		}

		static String readAgeDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																Txn txn, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				Get get = transactorSetup.txnOperationFactory.newGet(txn,key);
				OperationWithAttributes owa = transactorSetup.convertTestTypeGet(get,null);
//        addPredicateFilter(owa);
//				Object get = makeGet(dataLib, key);
//				transactorSetup.clientTransactor.initializeOperation(txn, (OperationWithAttributes)get);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Result rawTuple = reader.get(testSTable, owa);
						return readRawTuple(useSimple, transactorSetup, storeSetup, txn, name, dataLib, rawTuple, true, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		static String scanNoColumnsDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			Txn txn, String name, boolean deleted) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] endKey = dataLib.newRowKey(new Object[]{name});
				Scan s = transactorSetup.txnOperationFactory.newScan(txn);
				s.setStartRow(endKey);
				s.setStopRow(endKey);
				OperationWithAttributes scan = transactorSetup.convertTestTypeScan(s,null);
        addPredicateFilter(scan);
//				Object scan = makeScan(dataLib, endKey, families, endKey);
//				transactorSetup.clientTransactor.initializeOperation(txn, (OperationWithAttributes) scan);
				transactorSetup.readController.preProcessScan(scan);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Iterator<Result> results = reader.scan(testSTable, scan);
						Assert.assertTrue(deleted || results.hasNext());
//						if (!deleted) {
//								Assert.assertTrue(results.hasNext());
//						}
						Result rawTuple = results.next();
						Assert.assertTrue(!results.hasNext());
						return readRawTuple(useSimple, transactorSetup, storeSetup, txn, name, dataLib, rawTuple, false, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		private static String readRawTuple(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			 Txn txn, String name, SDataLib dataLib, Result rawTuple,
																			 boolean singleRowRead, 
																			 boolean dumpKeyValues, boolean decodeTimestamps)
						throws IOException {
				if (rawTuple != null) {
						Result result = rawTuple;
						if (useSimple) { // Process Results Through Filter
								TxnFilter filterState;
								try {
										filterState = transactorSetup.readController.newFilterState(transactorSetup.readResolver,txn);
								} catch (IOException e) {
										throw new RuntimeException(e);
								}
								EntryDecoder decoder = new EntryDecoder();
								filterState = new PackedTxnFilter(filterState, new HRowAccumulator(dataLib,EntryPredicateFilter.emptyPredicate(),decoder, false));
								result = transactorSetup.readController.filterResult(filterState, rawTuple);
								
						} 	
						if (result != null) {
								String suffix = dumpKeyValues ? "[ " + resultToKeyValueString(transactorSetup, dataLib, result, decodeTimestamps) + " ]" : "";
								String returnString = resultToStringDirect(transactorSetup, name, dataLib, result) + suffix;
								return returnString;
						}
				}
				if (singleRowRead) {
						return name + " absent";
				} else {
						return "";
				}
		}

		public String resultToString(String name, Result result) {
				return resultToStringDirect(transactorSetup, name, storeSetup.getDataLib(), result);
		}

		private static String resultToStringDirect(TestTransactionSetup transactorSetup, String name, SDataLib dataLib, Result result) {
				byte[] packedColumns = result.getValue(transactorSetup.family, SIConstants.PACKED_COLUMN_BYTES); // HERE JL
				EntryDecoder decoder = new EntryDecoder();
				decoder.set(packedColumns);
				MultiFieldDecoder mfd = null;
				try {
					mfd = decoder.getEntryDecoder();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Integer age = null;
				String job = null;
				if (decoder.isSet(0))
					age = mfd.decodeNextInt();
				if (decoder.isSet(1))					
					job = mfd.decodeNextString();
				String results = name + " age=" + age + " job=" + job;
				return results;
		}

		private static String resultToKeyValueString(TestTransactionSetup transactorSetup, SDataLib dataLib,
																								 Result result, boolean decodeTimestamps) {
			Map<Long, String> timestampDecoder = new HashMap<>();
			final StringBuilder s = new StringBuilder();
			byte[] packedColumns = result.getValue(transactorSetup.family, SIConstants.PACKED_COLUMN_BYTES);
			long timestamp = result.getColumnLatest(transactorSetup.family, SIConstants.PACKED_COLUMN_BYTES).getTimestamp();
			EntryDecoder decoder = new EntryDecoder();
			decoder.set(packedColumns);
			MultiFieldDecoder mfd = null;
			try {
				mfd = decoder.getEntryDecoder();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String value = null;
			if (decoder.isSet(0))
				s.append("V.age@"+timestampToStableString(timestampDecoder,timestamp)+ "="+mfd.decodeNextInt());
			if (decoder.isSet(1))	
				s.append("V.job@"+timestampToStableString(timestampDecoder,timestamp)+ "="+mfd.decodeNextString());
			return s.toString();	
		}

		public void assertWriteConflict(RetriesExhaustedWithDetailsException e) {
				Assert.assertEquals(1, e.getNumExceptions());
				Assert.assertTrue(e.getMessage().startsWith("Failed 1 action: com.splicemachine.si.impl.WriteConflict:"));
		}

		private static String timestampToStableString(Map<Long, String> timestampDecoder, long timestamp) {
				if (timestampDecoder.containsKey(timestamp)) {
						return timestampDecoder.get(timestamp);
				} else {
						final String timestampString = "~" + (9 - timestampDecoder.size());
						timestampDecoder.put(timestamp, timestampString);
						return timestampString;
				}
		}

		public Result readRaw(String name) throws IOException {
				return readAgeRawDirect(storeSetup, name, true);
		}

		public Result readRaw(String name, boolean allVersions) throws IOException {
				return readAgeRawDirect(storeSetup, name, allVersions);
		}

		static Result readAgeRawDirect(StoreSetup storeSetup, String name, boolean allversions) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes get = makeGet(dataLib, key);
				if (allversions) {
						dataLib.setGetMaxVersions(get);
				}
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						return reader.get(testSTable, get);
				} finally {
						reader.close(testSTable);
				}
		}

		public static OperationWithAttributes makeGet(SDataLib dataLib, byte[] key) throws IOException {
				final OperationWithAttributes get = dataLib.newGet(key, null, null, null);
				addPredicateFilter(get);
				return get;
		}

		private Object makeScan(SDataLib dataLib, byte[] endKey, ArrayList families, byte[] startKey) throws IOException {
				final OperationWithAttributes scan = (OperationWithAttributes)dataLib.newScan(startKey, endKey, families, null, null);
				addPredicateFilter(scan);
				return scan;
		}

		private static void addPredicateFilter(OperationWithAttributes operation) throws IOException {
				final BitSet bitSet = new BitSet(2);
				bitSet.set(0);
				bitSet.set(1);
				EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, new ObjectArrayList<Predicate>(), true);
				operation.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());
		}

}
