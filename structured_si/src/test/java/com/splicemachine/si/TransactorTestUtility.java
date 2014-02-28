package com.splicemachine.si;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HbRegion;
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
		StoreSetup storeSetup;
		TestTransactionSetup transactorSetup;
		Transactor transactor;
		TransactionManager control;
		SDataLib dataLib;
		static KryoPool kryoPool;
		
		static {
		        kryoPool = new KryoPool(SpliceConstants.kryoPoolSize);
		}

		public TransactorTestUtility(boolean useSimple,
																 StoreSetup storeSetup,
																 TestTransactionSetup transactorSetup,
																 Transactor transactor,
																 TransactionManager control) {
				this.useSimple = useSimple;
				this.storeSetup = storeSetup;
				this.transactorSetup = transactorSetup;
				this.transactor = transactor;
				this.control = control;
				dataLib = storeSetup.getDataLib();
		}

		public void insertAge(TransactionId transactionId, String name, Integer age) throws IOException {
				insertAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name, age);
		}

		public void insertAgeBatch(Object[]... args) throws IOException {
				insertAgeDirectBatch(useSimple, transactorSetup, storeSetup, args);
		}

		public void insertJob(TransactionId transactionId, String name, String job) throws IOException {
				insertJobDirect(useSimple, transactorSetup, storeSetup, transactionId, name, job);
		}

		public void deleteRow(TransactionId transactionId, String name) throws IOException {
				deleteRowDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
		}

		public String read(TransactionId transactionId, String name) throws IOException {
				return readAgeDirect(useSimple, transactorSetup, storeSetup, transactionId, name);
		}
		
		static void insertAgeDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
				TransactionId transactionId, String name, Integer age) throws IOException {
				insertField(useSimple, transactorSetup, storeSetup, transactionId, name, transactorSetup.agePosition, age);
		}

		static void insertAgeDirectBatch(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
						 Object[] args) throws IOException {
			insertFieldBatch(useSimple, transactorSetup, storeSetup, args, transactorSetup.agePosition);
		}

		static void insertJobDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
				TransactionId transactionId, String name, String job) throws IOException {
			insertField(useSimple, transactorSetup, storeSetup, transactionId, name, transactorSetup.jobPosition, job);
		}


		public String scan(TransactionId transactionId, String name) throws IOException {
				final STableReader reader = storeSetup.getReader();
				byte[] key = dataLib.newRowKey(new Object[]{name});
				Object scan = makeScan(dataLib, key, null, key);
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Iterator<Result> results = reader.scan(testSTable, scan);
						if (results.hasNext()) {
								Result rawTuple = results.next();
								Assert.assertTrue(!results.hasNext());
								return readRawTuple(useSimple, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, 
												true, true);
						} else {
								return "";
						}
				} finally {
						reader.close(testSTable);
				}
		}

		public String scanNoColumns(TransactionId transactionId, String name, boolean deleted) throws IOException {
				return scanNoColumnsDirect(useSimple, transactorSetup, storeSetup, transactionId, name, deleted);
		}

		public String scanAll(TransactionId transactionId, String startKey, String stopKey, Integer filterValue) throws IOException {
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
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), get);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						Iterator<Result> results = reader.scan(testSTable, get);

						StringBuilder result = new StringBuilder();
						while (results.hasNext()) {
								final Result value = results.next();
								final String name = Bytes.toString(value.getRow());
								final String s = readRawTuple(useSimple, transactorSetup, storeSetup, transactionId, name, dataLib, value, false,
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
																		TransactionId transactionId, String name, int index, Object fieldValue)
						throws IOException {
			final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				Object put = makePut(useSimple, transactorSetup, transactionId, name, index, fieldValue, dataLib);
				processPutDirect(useSimple, transactorSetup, storeSetup, reader, (OperationWithAttributes)put);
		}

		private static Object makePut(boolean useSimple, TestTransactionSetup transactorSetup, TransactionId transactionId, String name, int index, Object fieldValue, SDataLib dataLib) throws IOException {
				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes put = dataLib.newPut(key);
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
					dataLib.addKeyValueToPut(put, transactorSetup.family, dataLib.encode(SIConstants.PACKED_COLUMN_STRING), -1, entryEncoder.encode());
				} finally {
					entryEncoder.close();
				}
				transactorSetup.clientTransactor.initializePut(transactionId.getTransactionIdString(), put);
				return put;
		}

		private static void insertFieldBatch(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																				 Object[] args, int index) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();
				OperationWithAttributes[] puts = new OperationWithAttributes[args.length];
				int i = 0;
				for (Object subArgs : args) {
						Object[] subArgsArray = (Object[]) subArgs;
						TransactionId transactionId = (TransactionId) subArgsArray[0];
						String name = (String) subArgsArray[1];
						Object fieldValue = subArgsArray[2];
						Object put = makePut(useSimple, transactorSetup, transactionId, name, index, fieldValue, dataLib);
						puts[i] = (OperationWithAttributes)put;
						i++;
				}
				processPutDirectBatch(useSimple, transactorSetup, storeSetup, reader, puts);
		}

		static void deleteRowDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																TransactionId transactionId, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				OperationWithAttributes deletePut = transactorSetup.clientTransactor.createDeletePut(transactionId, key);
				processPutDirect(useSimple, transactorSetup, storeSetup, reader, deletePut);
		}

		private static void processPutDirect(boolean useSimple, 
																				 TestTransactionSetup transactorSetup, StoreSetup storeSetup, STableReader reader,
																				 OperationWithAttributes put) throws IOException {
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						if (useSimple) {
										Assert.assertTrue(transactorSetup.transactor.processPut(testSTable, transactorSetup.rollForwardQueue,
														((LTuple) put)));
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
								packedPuts[i] = ((LTuple) puts[i]);
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
																TransactionId transactionId, String name) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] key = dataLib.newRowKey(new Object[]{name});
				Object get = makeGet(dataLib, key);
				transactorSetup.clientTransactor.initializeGet(transactionId.getTransactionIdString(), get);
				Object testSTable = reader.open(storeSetup.getPersonTableName());
				try {
						System.out.println("get " + get);
						Result rawTuple = reader.get(testSTable, get);
						System.out.println("Reading Raw Tuple " + rawTuple);
						return readRawTuple(useSimple, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, true, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		static String scanNoColumnsDirect(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			TransactionId transactionId, String name, boolean deleted) throws IOException {
				final SDataLib dataLib = storeSetup.getDataLib();
				final STableReader reader = storeSetup.getReader();

				byte[] endKey = dataLib.newRowKey(new Object[]{name});
				final ArrayList families = new ArrayList();
				Object scan = makeScan(dataLib, endKey, families, endKey);
				transactorSetup.clientTransactor.initializeScan(transactionId.getTransactionIdString(), scan);
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
						return readRawTuple(useSimple, transactorSetup, storeSetup, transactionId, name, dataLib, rawTuple, false, false, true);
				} finally {
						reader.close(testSTable);
				}
		}

		private static String readRawTuple(boolean useSimple, TestTransactionSetup transactorSetup, StoreSetup storeSetup,
																			 TransactionId transactionId, String name, SDataLib dataLib, Result rawTuple,
																			 boolean singleRowRead, 
																			 boolean dumpKeyValues, boolean decodeTimestamps)
						throws IOException {
				if (rawTuple != null) {
						Result result = rawTuple;
						if (useSimple) { // Process Results Through Filter
								IFilterState filterState;
								try {
										filterState = transactorSetup.readController.newFilterState(transactorSetup.rollForwardQueue,transactionId);
								} catch (IOException e) {
										throw new RuntimeException(e);
								}
								EntryDecoder decoder = new EntryDecoder(kryoPool);
								filterState = new FilterStatePacked((FilterState) filterState, new HRowAccumulator(EntryPredicateFilter.emptyPredicate(),decoder));
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
				EntryDecoder decoder = new EntryDecoder(kryoPool);
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
			Map<Long, String> timestampDecoder = new HashMap<Long,String>();
			final StringBuilder s = new StringBuilder();
			byte[] packedColumns = result.getValue(transactorSetup.family, SIConstants.PACKED_COLUMN_BYTES);
			long timestamp = result.getColumnLatest(transactorSetup.family, SIConstants.PACKED_COLUMN_BYTES).getTimestamp();
			EntryDecoder decoder = new EntryDecoder(kryoPool);
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
