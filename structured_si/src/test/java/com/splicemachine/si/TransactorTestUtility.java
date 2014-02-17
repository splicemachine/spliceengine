package com.splicemachine.si;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.light.LRowAccumulator;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.impl.FilterState;
import com.splicemachine.si.impl.FilterStatePacked;
import com.splicemachine.si.impl.IFilterState;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 2/17/14
 */
public class TransactorTestUtility {
		boolean useSimple = true;
		boolean usePacked = false;

		StoreSetup storeSetup;
		TestTransactionSetup transactorSetup;
		Transactor transactor;
		TransactionManager control;

		public TransactorTestUtility(boolean useSimple,
																 boolean usePacked,
																 StoreSetup storeSetup,
																 TestTransactionSetup transactorSetup,
																 Transactor transactor,
																 TransactionManager control) {
				this.useSimple = useSimple;
				this.usePacked = usePacked;
				this.storeSetup = storeSetup;
				this.transactorSetup = transactorSetup;
				this.transactor = transactor;
				this.control = control;
		}

		public void insertAge(TransactionId transactionId, String name, Integer age) throws IOException {
				insertAgeDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, age);
		}

		public void insertAgeBatch(Object[]... args) throws IOException {
				insertAgeDirectBatch(useSimple, usePacked, transactorSetup, storeSetup, args);
		}

		public void insertJob(TransactionId transactionId, String name, String job) throws IOException {
				insertJobDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, job);
		}

		public void deleteRow(TransactionId transactionId, String name) throws IOException {
				deleteRowDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name);
		}

		public String read(TransactionId transactionId, String name) throws IOException {
				return readAgeDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name);
		}

		public String scan(TransactionId transactionId, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				Object scan = makeScan(dataLib, key, null, key);
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						Iterator<Result> results = reader.scan(testSTable, scan);
						if (results.hasNext()) {
								Result rawTuple = results.next();
								Assert.assertTrue(!results.hasNext());
								return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, true,
												true, true);
						} else {
								return "";
						}
				} finally {
						reader.close(testSTable);
				}
		}

		public String scanNoColumns(TransactionId transactionId, String name, boolean deleted) throws IOException {
				return scanNoColumnsDirect(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, deleted);
		}

		public String scanAll(TransactionId transactionId, String startKey, String stopKey, Integer filterValue,
													boolean includeSIColumn) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{startKey});
				byte[] endKey = dataLib.newRowKey(new Object[]{stopKey});
				Object get = makeScan(dataLib, endKey, null, key);
				if (!useSimple && filterValue != null) {
						final Scan scan = (Scan) get;
						SingleColumnValueFilter filter = new SingleColumnValueFilter((byte[]) transactorSetup.family,
										(byte[]) transactorSetup.ageQualifier,
										CompareFilter.CompareOp.EQUAL,
										new BinaryComparator(dataLib.encode(filterValue)));
						filter.setFilterIfMissing(true);
						scan.setFilter(filter);
				}
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get, includeSIColumn
				);
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						Iterator<Result> results = reader.scan(testSTable, get);

						StringBuilder result = new StringBuilder();
						while (results.hasNext()) {
								final Result value = results.next();
								final String name = Bytes.toString(value.getRow());
								final String s = readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, value, false,
												includeSIColumn, true, true);
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

		static void insertAgeDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																TransactionId transactionId, String name, Integer age) throws IOException {
				insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.ageQualifier, age);
		}

		static void insertAgeDirectBatch(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																		 Object[] args) throws IOException {
				insertFieldBatch(useSimple, usePacked, transactorSetup, storeSetup, args, transactorSetup.ageQualifier);
		}

		static void insertJobDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																TransactionId transactionId, String name, String job) throws IOException {
				insertField(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, transactorSetup.jobQualifier, job);
		}

		private static void insertField(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																		TransactionId transactionId, String name, byte[] qualifier, Object fieldValue)
						throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
				processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, (OperationWithAttributes)put);
		}

		private static Object makePut(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, TransactionId transactionId, String name, byte[] qualifier, Object fieldValue, SDataLib dataLib) throws IOException {
				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes put = dataLib.newPut(key);
				if (fieldValue != null) {
						if (usePacked && !useSimple) {
								int columnIndex;
								if(Bytes.equals(qualifier,transactorSetup.ageQualifier)){
										columnIndex = 0;
								} else if (Bytes.equals(qualifier,transactorSetup.jobQualifier)){
										columnIndex = 1;
								} else {
										throw new RuntimeException("unknown qualifier");
								}
								final BitSet bitSet = new BitSet();
								bitSet.set(columnIndex);
								final EntryEncoder entryEncoder = EntryEncoder.create(KryoPool.defaultPool(), 2, bitSet, null, null, null);
								try {
										if (columnIndex == 0) {
												entryEncoder.getEntryEncoder().encodeNext((Integer) fieldValue);
										} else {
												entryEncoder.getEntryEncoder().encodeNext((String) fieldValue);
										}
										final byte[] packedRow = entryEncoder.encode();
										dataLib.addKeyValueToPut(put, transactorSetup.family, dataLib.encode("x"), -1, packedRow);
								} finally {
										entryEncoder.close();
								}
						} else {
								dataLib.addKeyValueToPut(put, transactorSetup.family, qualifier, -1l, dataLib.encode(fieldValue));
						}
				}
				transactorSetup.clientTransactor.initializePut(transactionId.getTransactionIdString(), put);
				return put;
		}

		private static void insertFieldBatch(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																				 Object[] args, byte[] qualifier) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				OperationWithAttributes[] puts = new OperationWithAttributes[args.length];
				int i = 0;
				for (Object subArgs : args) {
						Object[] subArgsArray = (Object[]) subArgs;
						TransactionId transactionId = (TransactionId) subArgsArray[0];
						String name = (String) subArgsArray[1];
						Object fieldValue = subArgsArray[2];
						Object put = makePut(useSimple, usePacked, transactorSetup, transactionId, name, qualifier, fieldValue, dataLib);
						puts[i] = (OperationWithAttributes)put;
						i++;
				}
				processPutDirectBatch(useSimple, usePacked, transactorSetup, storeSetup, reader, puts);
		}

		static void deleteRowDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																TransactionId transactionId, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes deletePut = transactorSetup.clientTransactor.createDeletePut(transactionId, key);
				processPutDirect(useSimple, usePacked, transactorSetup, storeSetup, reader, deletePut);
		}

		private static void processPutDirect(boolean useSimple, boolean usePacked,
																				 TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
																				 OperationWithAttributes put) throws IOException {
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						if (useSimple) {
								if (usePacked) {
										Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue,
														((LTuple) put).pack(Bytes.toBytes("V"), Bytes.toBytes("p"))));
								} else {
										Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue, put));
								}
						} else {
								storeSetup.getWriter().write(testSTable, put);
						}
				} finally {
						reader.close(testSTable);
				}
		}

		private static void processPutDirectBatch(boolean useSimple, boolean usePacked,
																							TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
																							OperationWithAttributes[] puts) throws IOException {
				final Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						Object tableToUse = testSTable;
						if (useSimple) {
								if (usePacked) {
										OperationWithAttributes[] packedPuts = new OperationWithAttributes[puts.length];
										for (int i = 0; i < puts.length; i++) {
												packedPuts[i] = ((LTuple) puts[i]).pack(Bytes.toBytes("V"), Bytes.toBytes("p"));
										}
										puts = packedPuts;
								}
						} else {
								tableToUse = new HbRegion(HStoreSetup.regionMap.get(storeSetup.getPersonTableName(usePacked)));
						}
						final Object[] results = transactorSetup.transactor.processPutBatch(tableToUse, transactorSetup.rollForwardQueue, puts);
						for (Object r : results) {
								Assert.assertEquals(HConstants.OperationStatusCode.SUCCESS, ((OperationStatus) r).getOperationStatusCode());
						}
				} finally {
						reader.close(testSTable);
				}
		}

		static String readAgeDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																TransactionId transactionId, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				Object get = makeGet(dataLib, key);
				transactorSetup.clientTransactor.initializeGet(transactionId.getTransactionIdString(), get);
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						Result rawTuple = reader.get(testSTable, get);
						return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, true, true, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		static String scanNoColumnsDirect(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			TransactionId transactionId, String name, boolean deleted) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] endKey = dataLib.newRowKey(new Object[]{name});
				final ArrayList families = new ArrayList();
				Object scan = makeScan(dataLib, endKey, families, endKey);
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan, true);
				transactorSetup.readController.preProcessScan(scan);
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						Iterator<Result> results = reader.scan(testSTable, scan);
						Assert.assertTrue(deleted || results.hasNext());
//						if (!deleted) {
//								Assert.assertTrue(results.hasNext());
//						}
						Result rawTuple = results.next();
						Assert.assertTrue(!results.hasNext());
						return readRawTuple(useSimple, usePacked, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, true, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		private static String readRawTuple(boolean useSimple, boolean usePacked, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			 TransactionId transactionId, String name, SDataLib dataLib, Result rawTuple,
																			 boolean singleRowRead, boolean includeSIColumn,
																			 boolean dumpKeyValues, boolean decodeTimestamps)
						throws IOException {
				if (rawTuple != null) {
						Result result = rawTuple;
						if (useSimple) {
								IFilterState filterState;
								try {
										filterState = transactorSetup.readController.newFilterState(transactorSetup.rollForwardQueue,
														transactionId, includeSIColumn);
								} catch (IOException e) {
										throw new RuntimeException(e);
								}
								if (usePacked) {
										filterState = new FilterStatePacked(null, storeSetup.getDataLib(), transactorSetup.dataStore, (FilterState) filterState, new LRowAccumulator());
								}
								result = transactorSetup.readController.filterResult(filterState, rawTuple);
								if (usePacked && result != null) {
										List<KeyValue> kvs = Lists.newArrayList(result.raw());
										result = new Result(LTuple.unpack(kvs,Bytes.toBytes("V")));
//										result = ((LTuple) result).unpack(Bytes.toBytes("V"));
								}
						} else {
								if (result.size() == 0) {
										return name + " absent";
								}
								if (usePacked) {
										byte[] resultKey = result.getRow();
										final List newKeyValues = new ArrayList();
										List<KeyValue> list = dataLib.listResult(result);
										for (KeyValue kv : list) {
												if(kv.matchingColumn(transactorSetup.family,dataLib.encode("x"))){
//                        if (Bytes.equals(dataLib.getKeyValueFamily(kv), transactorSetup.family) && Bytes.equals(dataLib.getKeyValueQualifier(kv), dataLib.encode("x"))) {
														final byte[] packedColumns = kv.getValue();
														final EntryDecoder entryDecoder = new EntryDecoder(KryoPool.defaultPool());
														entryDecoder.set(packedColumns);
														final MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
														if (entryDecoder.isSet(0)) {
																final int age = decoder.decodeNextInt();
																final KeyValue ageKeyValue = KeyValueUtils.newKeyValue(resultKey, transactorSetup.family,
																				transactorSetup.ageQualifier,
																				kv.getTimestamp(),
																				dataLib.encode(age));
																newKeyValues.add(ageKeyValue);
														}
														if (entryDecoder.isSet(1)) {
																final String job = decoder.decodeNextString();
																final KeyValue jobKeyValue = KeyValueUtils.newKeyValue(resultKey, transactorSetup.family,
																				transactorSetup.jobQualifier,
																				kv.getTimestamp(),
																				dataLib.encode(job));
																newKeyValues.add(jobKeyValue);
														}
												} else {
														newKeyValues.add(kv);
												}
										}
										result = new Result(newKeyValues);
								}
						}
						if (result != null) {
								String suffix = dumpKeyValues ? "[ " + resultToKeyValueString(transactorSetup, dataLib, result, decodeTimestamps) + "]" : "";
								return resultToStringDirect(transactorSetup, name, dataLib, result) + suffix;
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
				final byte[] ageValue = result.getValue(transactorSetup.family, transactorSetup.ageQualifier);

				Integer age = (Integer) dataLib.decode(ageValue, Integer.class);
				final byte[] jobValue = result.getValue(transactorSetup.family, transactorSetup.jobQualifier);
				String job = (String) dataLib.decode(jobValue, String.class);
				return name + " age=" + age + " job=" + job;
		}

		private static String resultToKeyValueString(TestTransactionSetup transactorSetup, SDataLib dataLib,
																								 Result result, boolean decodeTimestamps) {
				final Map<Long, String> timestampDecoder = new HashMap<Long, String>();
				final StringBuilder s = new StringBuilder();
				List<KeyValue> list = dataLib.listResult(result);
				for (KeyValue kv : list) {
						final byte[] family = kv.getFamily();
						String qualifier = "?";
						String value = "?";
						if(kv.matchingQualifier(transactorSetup.ageQualifier)){
								value = dataLib.decode(kv.getValue(), Integer.class).toString();
								qualifier = "age";
						} else if(kv.matchingQualifier(transactorSetup.jobQualifier)){
								value = (String) dataLib.decode(kv.getValue(), String.class);
								qualifier = "job";
						} else if(kv.matchingQualifier(transactorSetup.tombstoneQualifier)){
								qualifier = "X";
								value = "";
						} else if(kv.matchingQualifier(transactorSetup.commitTimestampQualifier)){
								qualifier = "TX";
								value = "";
						}
						final long timestamp = kv.getTimestamp();
						String timestampString;
						if (decodeTimestamps) {
								timestampString = timestampToStableString(timestampDecoder, timestamp);
						} else {
								timestampString = Long.toString(timestamp);
						}
						String equalString = value.length() > 0 ? "=" : "";
						s.append(Bytes.toString(family))
										.append(".").append(qualifier)
										.append("@").append(timestampString)
										.append(equalString).append(value).append(" ");
				}
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

		public Result readRaw(String name,boolean usePacked) throws IOException {
				return readAgeRawDirect(storeSetup, name, true,usePacked);
		}

		public Result readRaw(String name, boolean allVersions,boolean usePacked) throws IOException {
				return readAgeRawDirect(storeSetup, name, allVersions,usePacked);
		}

		static Result readAgeRawDirect(StoreSetup storeSetup, String name, boolean allversions,boolean usePacked) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes get = makeGet(dataLib, key);
				if (allversions) {
						dataLib.setGetMaxVersions(get);
				}
				Object testSTable = reader.open(storeSetup.getPersonTableName(usePacked));
				try {
						return reader.get(testSTable, get);
				} finally {
						reader.close(testSTable);
				}
		}

		public static OperationWithAttributes makeGet(SDataLib dataLib, byte[] key) throws IOException {
				final OperationWithAttributes get = dataLib.newGet(key, null, null, null);
				addPredicateFilter(dataLib, get);
				return get;
		}

		private static Object makeScan(SDataLib dataLib, byte[] endKey, ArrayList families, byte[] startKey) throws IOException {
				final Object scan = dataLib.newScan(startKey, endKey, families, null, null);
				addPredicateFilter(dataLib, scan);
				return scan;
		}

		private static void addPredicateFilter(SDataLib dataLib, Object operation) throws IOException {
				final BitSet bitSet = new BitSet(2);
				bitSet.set(0);
				bitSet.set(1);
				EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, new ObjectArrayList<Predicate>(), true);
				((OperationWithAttributes)operation).setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,filter.toBytes());
		}

}
