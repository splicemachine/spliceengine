package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnTestUtils {
		private TxnTestUtils(){}

		public static void assertTxnsMatch(String baseErrorMessage, SparseTxn correct, SparseTxn actual) {
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", correct.getTxnId(), actual.getTxnId());
				Assert.assertEquals(baseErrorMessage + " Parent txn ids differ", correct.getParentTxnId(), actual.getParentTxnId());
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", correct.getBeginTimestamp(), actual.getBeginTimestamp());
				Assert.assertEquals(baseErrorMessage + " HasDependent property differs", correct.hasDependentField(), actual.hasDependentField());
				Assert.assertEquals(baseErrorMessage + " Dependent property differs", correct.isDependent(), actual.isDependent());
				Assert.assertEquals(baseErrorMessage + " HasAdditive property differs", correct.hasAdditiveField(), actual.hasAdditiveField());
				Assert.assertEquals(baseErrorMessage + " Additive property differs", correct.isAdditive(), actual.isAdditive());
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", correct.getIsolationLevel(), actual.getIsolationLevel());
				Assert.assertEquals(baseErrorMessage + " State differs", correct.getState(), actual.getState());
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", correct.getCommitTimestamp(), actual.getCommitTimestamp());
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", correct.getGlobalCommitTimestamp(), actual.getGlobalCommitTimestamp());
				ByteSlice correctDestTable = correct.getDestinationTableBuffer();
				ByteSlice actualDestTable = actual.getDestinationTableBuffer();
				if(correctDestTable==null || correctDestTable.length()<=0){
						if(actualDestTable!=null)
								Assert.assertTrue(baseErrorMessage + " Destination table differs. Should be empty, but has length " + actualDestTable.length(), actualDestTable.length() <= 0);
						//if actualDestTable==null, then it matches
				}  else{
						Assert.assertEquals(baseErrorMessage + " Destination table differs", correct.getDestinationTableBuffer(), actual.getDestinationTableBuffer());
				}
		}

		public static void assertTxnsMatch(String baseErrorMessage, Txn correct, Txn actual) {
				if(correct==actual) return; //they are the same object
				Assert.assertEquals(baseErrorMessage + " TxnIds differ", correct.getTxnId(), actual.getTxnId());
				assertTxnsMatch(baseErrorMessage + " Parent txns differ: ", correct.getParentTransaction(), actual.getParentTransaction());
				Assert.assertEquals(baseErrorMessage + " Begin timestamps differ", correct.getBeginTimestamp(), actual.getBeginTimestamp());
				Assert.assertEquals(baseErrorMessage + " Dependent property differs", correct.isDependent(), actual.isDependent());
				Assert.assertEquals(baseErrorMessage + " Additive property differs", correct.isAdditive(), actual.isAdditive());
				Assert.assertEquals(baseErrorMessage + " Isolation level differs", correct.getIsolationLevel(), actual.getIsolationLevel());
				Assert.assertEquals(baseErrorMessage + " State differs", correct.getState(), actual.getState());
				Assert.assertEquals(baseErrorMessage + " Commit timestamp differs", correct.getCommitTimestamp(), actual.getCommitTimestamp());
				Assert.assertEquals(baseErrorMessage + " Global Commit timestamp differs", correct.getGlobalCommitTimestamp(), actual.getGlobalCommitTimestamp());
				List<byte[]> correctTables = Lists.newArrayList(correct.getDestinationTables());
				List<byte[]> actualTables = Lists.newArrayList(actual.getDestinationTables());

				Collections.sort(correctTables, Bytes.BYTES_COMPARATOR);
				Collections.sort(actualTables,Bytes.BYTES_COMPARATOR);

				Assert.assertEquals(baseErrorMessage+ " Incorrect destination table size!",correctTables.size(),actualTables.size());
				for(int i=0;i<correctTables.size();i++){
						byte[] correctBytes = correctTables.get(i);
						byte[] actualBytes = actualTables.get(i);
						Assert.assertArrayEquals(baseErrorMessage+" Incorrect destination table at position "+ i,correctBytes,actualBytes);
				}
		}
}
