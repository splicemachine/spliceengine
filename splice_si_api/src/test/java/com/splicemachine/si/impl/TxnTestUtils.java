/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;

import org.junit.Assert;
/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnTestUtils{
    /**
     TODO JL : Wrong Place
    public TxnTestUtils(){
    }

    public static void assertTxnsMatch(String baseErrorMessage,TxnMessage.Txn correct,TxnMessage.Txn actual){
        assertTxnInfoMatch(baseErrorMessage,correct.getInfo(),actual.getInfo());
        Assert.assertEquals(baseErrorMessage+" State differs",correct.getState(),actual.getState());
        Assert.assertEquals(baseErrorMessage+" Commit timestamp differs",correct.getCommitTs(),actual.getCommitTs());
        Assert.assertEquals(baseErrorMessage+" Global Commit timestamp differs",correct.getGlobalCommitTs(),actual.getGlobalCommitTs());
    }


    public static void assertTxnsMatch(String baseErrorMessage,TxnView correct,TxnView actual){
        if(correct==actual) return; //they are the same object
        Assert.assertEquals(baseErrorMessage+" TxnIds differ",correct.getTxnId(),actual.getTxnId());
        assertTxnsMatch(baseErrorMessage+" Parent txns differ: ",correct.getParentTxnView(),actual.getParentTxnView());
        Assert.assertEquals(baseErrorMessage+" Begin timestamps differ",correct.getBeginTimestamp(),actual.getBeginTimestamp());
        Assert.assertEquals(baseErrorMessage+" Additive property differs",correct.isAdditive(),actual.isAdditive());
        Assert.assertEquals(baseErrorMessage+" Isolation level differs",correct.getIsolationLevel(),actual.getIsolationLevel());
        Assert.assertEquals(baseErrorMessage+" State differs",correct.getState(),actual.getState());
        Assert.assertEquals(baseErrorMessage+" Commit timestamp differs",correct.getCommitTimestamp(),actual.getCommitTimestamp());
        Assert.assertEquals(baseErrorMessage+" Global Commit timestamp differs",correct.getGlobalCommitTimestamp(),actual.getGlobalCommitTimestamp());
//        List<ByteSlice> correctTables=Lists.newArrayList(correct.getDestinationTables());
//        List<ByteSlice> actualTables=Lists.newArrayList(actual.getDestinationTables());

//        Comparator<ByteSlice> sliceComparator=new Comparator<ByteSlice>(){
//            @Override
//            public int compare(ByteSlice o1,ByteSlice o2){
//                if(o1==null){
//                    if(o2==null) return 0;
//                    return -1;
//                }else if(o2==null)
//                    return 1;
//                else{
//                    return Bytes.basicByteComparator().compare(o1.array(),o1.offset(),o1.length(),o2.array(),o2.offset(),o2.length());
//                }
//            }
//        };
//        Collections.sort(correctTables,sliceComparator);
//        Collections.sort(actualTables,sliceComparator);
//
//        Assert.assertEquals(baseErrorMessage+" Incorrect destination table size!",correctTables.size(),actualTables.size());
//        for(int i=0;i<correctTables.size();i++){
//            ByteSlice correctBytes=correctTables.get(i);
//            ByteSlice actualBytes=actualTables.get(i);
//            Assert.assertEquals(baseErrorMessage+" Incorrect destination table at position "+i,correctBytes,actualBytes);
//        }
    }

    private static void assertTxnInfoMatch(String baseErrorMessage,TxnMessage.TxnInfo correct,TxnMessage.TxnInfo actual){
        Assert.assertEquals(baseErrorMessage+" TxnIds differ",correct.getTxnId(),actual.getTxnId());
        Assert.assertEquals(baseErrorMessage+" Parent txn ids differ",correct.getParentTxnid(),actual.getParentTxnid());
        Assert.assertEquals(baseErrorMessage+" Begin timestamps differ",correct.getBeginTs(),actual.getBeginTs());
        Assert.assertEquals(baseErrorMessage+" HasAdditive property differs",correct.hasIsAdditive(),actual.hasIsAdditive());
        Assert.assertEquals(baseErrorMessage+" Additive property differs",correct.getIsAdditive(),actual.getIsAdditive());
        Assert.assertEquals(baseErrorMessage+" Isolation level differs",correct.getIsolationLevel(),actual.getIsolationLevel());
//        Assert.assertEquals(baseErrorMessage+" Table buffer differs",correct.getDestinationTables(),actual.getDestinationTables());
    }
    */
}
