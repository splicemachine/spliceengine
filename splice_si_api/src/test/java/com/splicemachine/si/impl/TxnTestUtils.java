/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnTestUtils{

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

    public static TxnView getMockCommittedTxn(long begin, long commit, TxnView parent){
        assert parent != null;
        return new InheritingTxnView(parent,begin,begin,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                false,false,
                true,true,
                commit,-1l,
                Txn.State.COMMITTED);
    }

    public static TxnView getMockCommittedTxn(long begin, long commit) {
        return getMockCommittedTxn(begin, commit, Txn.ROOT_TRANSACTION);
    }

    public static TxnView getMockRolledBackTxn(long begin, TxnView parent) {
        assert parent != null;
        return new InheritingTxnView(parent,
                begin,begin,true,
                Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ROLLEDBACK);
    }

    public static TxnView getMockRolledBackTxn(long begin) {
        return getMockRolledBackTxn(begin, Txn.ROOT_TRANSACTION);
    }

    public static TxnView getMockActiveTxn(long begin, TxnView parent) {
        assert parent != null;
        return new InheritingTxnView(parent,begin,begin,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.State.ACTIVE);
    }

    public static TxnView getMockActiveTxn(long begin) {
        return getMockActiveTxn(begin, Txn.ROOT_TRANSACTION);
    }
}
