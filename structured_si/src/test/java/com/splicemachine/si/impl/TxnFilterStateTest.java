package com.splicemachine.si.impl;

import com.google.common.collect.Maps;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class TxnFilterStateTest {

		@Test
		public void testCanSeeRow() throws Exception {
				final Map<Long,Txn> txnMap = Maps.newHashMap();
				TxnAccess baseStore = getMapStore(txnMap);

				//add a committed transaction to the map
				Txn committed = new CommittedTxn(0,1);
				txnMap.put(0l,committed);

				DataStore ds = mock(DataStore.class);
				when(ds.getKeyValueType(any(KeyValue.class))).thenAnswer(new Answer<KeyValueType>() {
						@Override
						public KeyValueType answer(InvocationOnMock invocationOnMock) throws Throwable {
								KeyValue arg = (KeyValue)invocationOnMock.getArguments()[0];
								if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.COMMIT_TIMESTAMP;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.TOMBSTONE;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,arg.getQualifier()))
										return KeyValueType.ANTI_TOMBSTONE;
								else if(Bytes.equals(SpliceConstants.PACKED_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.USER_DATA;
								else return KeyValueType.OTHER;
						}
				});

				Txn myTxn = new InheritingTxnView(Txn.ROOT_TRANSACTION,2l,2l, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE);
				TxnFilterState filterState = new TxnFilterState(baseStore,myTxn,
								NoOpRollForwardQueue.INSTANCE,ds);

				KeyValue testCommitKv = new KeyValue(Encoding.encode("1"), SpliceConstants.DEFAULT_FAMILY_BYTES,
								SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, 0l,Bytes.toBytes(1l));

				Filter.ReturnCode code = filterState.filterKeyValue(testCommitKv);
				Assert.assertEquals("Incorrect return code for commit keyvalue!", Filter.ReturnCode.SKIP, code);

				KeyValue testDataKv = new KeyValue(Encoding.encode("1"),SpliceConstants.DEFAULT_FAMILY_BYTES,
								SpliceConstants.PACKED_COLUMN_BYTES,0l,Encoding.encode("hello"));

				Filter.ReturnCode returnCode = filterState.filterKeyValue(testDataKv);
				Assert.assertEquals("Incorrect return code for data keyvalue!", Filter.ReturnCode.INCLUDE,returnCode);
		}

		@Test
		public void testCannotSeeRolledBackRow() throws Exception {
				final Map<Long,Txn> txnMap = Maps.newHashMap();
				TxnAccess baseStore = getMapStore(txnMap);

				//add a rolledBack transaction to the map
				Txn rolledBack = new RolledBackTxn(0l);
				txnMap.put(0l,rolledBack);

				DataStore ds = mock(DataStore.class);
				when(ds.getKeyValueType(any(KeyValue.class))).thenAnswer(new Answer<KeyValueType>() {
						@Override
						public KeyValueType answer(InvocationOnMock invocationOnMock) throws Throwable {
								KeyValue arg = (KeyValue)invocationOnMock.getArguments()[0];
								if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.COMMIT_TIMESTAMP;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.TOMBSTONE;
								else if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,arg.getQualifier()))
										return KeyValueType.ANTI_TOMBSTONE;
								else if(Bytes.equals(SpliceConstants.PACKED_COLUMN_BYTES,arg.getQualifier()))
										return KeyValueType.USER_DATA;
								else return KeyValueType.OTHER;
						}
				});

				Txn myTxn = new InheritingTxnView(Txn.ROOT_TRANSACTION,2l,2l, Txn.IsolationLevel.SNAPSHOT_ISOLATION, Txn.State.ACTIVE);
				TxnFilterState filterState = new TxnFilterState(baseStore,myTxn,
								NoOpRollForwardQueue.INSTANCE,ds);

//				KeyValue testCommitKv = new KeyValue(Encoding.encode("1"), SpliceConstants.DEFAULT_FAMILY_BYTES,
//								SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, 0l,Bytes.toBytes(1l));
//
//				Filter.ReturnCode code = filterState.filterKeyValue(testCommitKv);
//				Assert.assertEquals("Incorrect return code for commit keyvalue!", Filter.ReturnCode.SKIP, code);

				KeyValue testDataKv = new KeyValue(Encoding.encode("1"),SpliceConstants.DEFAULT_FAMILY_BYTES,
								SpliceConstants.PACKED_COLUMN_BYTES,0l,Encoding.encode("hello"));

				Filter.ReturnCode returnCode = filterState.filterKeyValue(testDataKv);
				Assert.assertEquals("Incorrect return code for data keyvalue!", Filter.ReturnCode.SKIP,returnCode);
		}

		protected TxnAccess getMapStore(final Map<Long, Txn> txnMap) throws IOException {
				TxnAccess baseStore = mock(TxnAccess.class);
				when(baseStore.getTransaction(anyLong())).thenAnswer(new Answer<Txn>() {
						@Override
						public Txn answer(InvocationOnMock invocationOnMock) throws Throwable {
								return txnMap.get((Long) invocationOnMock.getArguments()[0]);
						}
				});
				return baseStore;
		}

}
