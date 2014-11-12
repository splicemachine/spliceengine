package com.splicemachine.si.impl;

import com.google.common.collect.*;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import java.util.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnTestUtils {
		public static STransactionLib transactionLib = SIFactoryDriver.siFactory.getTransactionLib();
		public TxnTestUtils(){}

		public static <Transaction,TableBuffer> void assertTxnsMatch(String baseErrorMessage, Transaction correct, Transaction actual) {
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", transactionLib.getTxnId(correct), transactionLib.getTxnId(actual));
				Assert.assertEquals(baseErrorMessage + " Parent txn ids differ", transactionLib.getParentTxnId(correct), transactionLib.getParentTxnId(actual));
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", transactionLib.getBeginTimestamp(correct), transactionLib.getBeginTimestamp(actual));
				Assert.assertEquals(baseErrorMessage + " HasAdditive property differs", transactionLib.hasAddiditiveField(correct), transactionLib.hasAddiditiveField(actual));
				Assert.assertEquals(baseErrorMessage + " Additive property differs", transactionLib.isAddiditive(correct), transactionLib.isAddiditive(actual));
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", transactionLib.getIsolationLevel(correct), transactionLib.getIsolationLevel(actual));
				Assert.assertEquals(baseErrorMessage + " State differs", transactionLib.getTransactionState(correct), transactionLib.getTransactionState(actual));
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", transactionLib.getCommitTimestamp(correct), transactionLib.getCommitTimestamp(actual));
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", transactionLib.getGlobalCommitTimestamp(correct), transactionLib.getGlobalCommitTimestamp(actual));
				Assert.assertEquals(baseErrorMessage + " Table buffer differs", transactionLib.getDestinationTableBuffer(correct), transactionLib.getDestinationTableBuffer(actual));
		}

		public static void assertTxnsMatch(String baseErrorMessage, TxnView correct, TxnView actual) {
				if(correct==actual) return; //they are the same object
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", correct.getTxnId(), actual.getTxnId());
				assertTxnsMatch(baseErrorMessage + " Parent txns differ: ", correct.getParentTxnView(), actual.getParentTxnView());
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", correct.getBeginTimestamp(), actual.getBeginTimestamp());
				Assert.assertEquals(baseErrorMessage + " Additive property differs", correct.isAdditive(), actual.isAdditive());
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", correct.getIsolationLevel(), actual.getIsolationLevel());
				Assert.assertEquals(baseErrorMessage + " State differs", correct.getState(), actual.getState());
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", correct.getCommitTimestamp(), actual.getCommitTimestamp());
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", correct.getGlobalCommitTimestamp(), actual.getGlobalCommitTimestamp());
				List<ByteSlice> correctTables = Lists.newArrayList(correct.getDestinationTables());
				List<ByteSlice> actualTables = Lists.newArrayList(actual.getDestinationTables());

        Comparator<ByteSlice> sliceComparator = new Comparator<ByteSlice>(){
            @Override
            public int compare(ByteSlice o1, ByteSlice o2) {
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null)
                    return 1;
                else{
                    return Bytes.compareTo(o1.array(),o1.offset(),o1.length(),o2.array(),o2.offset(),o2.length());
                }
            }
        };
				Collections.sort(correctTables, sliceComparator);
				Collections.sort(actualTables,sliceComparator);

				Assert.assertEquals(baseErrorMessage+ " Incorrect destination table size!",correctTables.size(),actualTables.size());
				for(int i=0;i<correctTables.size();i++){
						ByteSlice correctBytes = correctTables.get(i);
						ByteSlice actualBytes = actualTables.get(i);
						Assert.assertEquals(baseErrorMessage+" Incorrect destination table at position "+ i,correctBytes,actualBytes);
				}
		}

		public static DataStore getMockDataStore() {
				DataStore ds = mock(DataStore.class);
				when(ds.getKeyValueType(any(KeyValue.class))).thenAnswer(new Answer<KeyValueType>() {
						@Override
						public KeyValueType answer(InvocationOnMock invocationOnMock) throws Throwable {
								KeyValue arg = (KeyValue)invocationOnMock.getArguments()[0];
								if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, arg.getQualifier()))
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
				when(ds.getDataLib()).thenReturn(new LDataLib()); 
				return ds;
		}


}
