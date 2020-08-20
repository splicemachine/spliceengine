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

package com.splicemachine.pipeline;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.UniqueConstraint;
import com.splicemachine.pipeline.constraint.UniqueConstraintViolation;
import com.splicemachine.pipeline.contextfactory.ConstraintFactory;
import com.splicemachine.pipeline.testsetup.PipelineTestEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.storage.*;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
@Category(ArchitectureSpecific.class)
public class PipelineTest{
    private static final String DESTINATION_TABLE=Long.toString(1232);
    private static final byte[] DESTINATION_TABLE_BYTES=Bytes.toBytes(DESTINATION_TABLE);
    private static PipelineTestEnv testEnv;
    private static TestTransactionSetup tts;
    private static TxnLifecycleManager lifecycleManager;
    private static final KryoPool kp = new KryoPool(1);


    @Before
    public void setUp() throws Exception{
       if(testEnv==null){
           testEnv=PipelineTestEnvironment.loadTestEnvironment();
           tts = new TestTransactionSetup(testEnv,false);
           lifecycleManager =tts.txnLifecycleManager;
           //add a unique constraint
           ConstraintContext cc = ConstraintContext.unique("data","unique");
           Constraint c = new UniqueConstraint(cc,testEnv.getOperationStatusFactory());
           ConstraintFactory cf = new ConstraintFactory(c,testEnv.pipelineExceptionFactory());
           testEnv.contextFactoryLoader(1232).getConstraintFactories().add(cf);

           testEnv.createTransactionalTable(DESTINATION_TABLE_BYTES);
       }
    }

    @Test
    public void insertSingleRecord() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn)){
            KVPair data = encode("scott1",null,29);
            callBuffer.add(data);
            callBuffer.flushBufferAndWait();

            DataGet dg = testEnv.getOperationFactory().newDataGet(txn,data.getRowKey(),null);
            try(Partition p = testEnv.getPartition(DESTINATION_TABLE,tts)){
                DataResult result=p.get(dg,null);
                assertCorrectPresence(txn,data,result);
            }
        }finally{
            txn.rollback();
        }
    }

    @Test
    public void insertDeleteSingleRecord() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn)){
            KVPair data = encode("scott2",null,29);
            callBuffer.add(data);
            callBuffer.flushBufferAndWait();

            DataGet dg = testEnv.getOperationFactory().newDataGet(txn,data.getRowKey(),null);
            try(Partition p = testEnv.getPartition(DESTINATION_TABLE,tts)){
                DataResult result=p.get(dg,null);
                assertCorrectPresence(txn,data,result);
            }

            data.setType(KVPair.Type.DELETE);
            callBuffer.add(data);
            callBuffer.flushBufferAndWait();
            dg = testEnv.getOperationFactory().newDataGet(txn,data.getRowKey(),null);
            try(Partition p = testEnv.getPartition(DESTINATION_TABLE,tts)){
                DataResult dr = p.get(dg,null);
                Assert.assertTrue("Row was still found!",dr==null || dr.isEmpty());
            }
        }finally{
            txn.rollback();
        }
    }

    @Test
    public void insertManyRecords() throws Exception{
        int numRecords = 100;
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn)){
            List<KVPair> data=new ArrayList<>(numRecords);
            for(int i=0;i<numRecords;i++){
                KVPair kvP=encode("ryan"+i,null,i);
                callBuffer.add(kvP);
                data.add(kvP);
            }
            callBuffer.flushBufferAndWait();

            //make sure that all the rows are present
            DataGet dg= null;
            DataResult result = null;
            for(KVPair d : data){
                dg=testEnv.getOperationFactory().newDataGet(txn,d.getRowKey(),dg);
                try(Partition p=testEnv.getPartition(DESTINATION_TABLE,tts)){
                    result=p.get(dg,result);
                    assertCorrectPresence(txn,d,result);
                }
            }
        }finally{
            txn.rollback();
        }
    }

    @Test
    public void insertManyRecordsDeleteSome() throws Exception{
        int numRecords = 100;
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn)){
            List<KVPair> data=new ArrayList<>(numRecords);
            for(int i=1000;i<1000+numRecords;i++){
                KVPair kvP=encode("ryan"+i,null,i);
                callBuffer.add(kvP);
                data.add(kvP);
            }
            callBuffer.flushBufferAndWait();

            DataGet dg= null;
            DataResult result = null;
            for(KVPair d : data){
                dg=testEnv.getOperationFactory().newDataGet(txn,d.getRowKey(),dg);
                try(Partition p=testEnv.getPartition(DESTINATION_TABLE,tts)){
                    result=p.get(dg,result);
                    assertCorrectPresence(txn,d,result);
                }
            }

            int i=0;
            for(KVPair kvP:data){
                if(i%2==0){
                    kvP.setType(KVPair.Type.DELETE);
                    callBuffer.add(kvP);
                }
                i++;
            }
            callBuffer.flushBufferAndWait();
            i=0;
            try(Partition p=testEnv.getPartition(DESTINATION_TABLE,tts)){
                for(KVPair d : data){
                    dg=testEnv.getOperationFactory().newDataGet(txn,d.getRowKey(),dg);
                    result=p.get(dg,result);
                    if(i%2==0){
                        Assert.assertTrue("Row was deleted, but still found!",result==null||result.isEmpty());
                    }else{
                        assertCorrectPresence(txn,d,result);
                    }
                    i++;
                }
            }
        }finally{
            txn.rollback();
        }
    }

    @Test
    public void writeWriteConflict() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn1 =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn1)){
            KVPair data = encode("scott3",null,29);
            callBuffer.add(data);
            callBuffer.flushBufferAndWait();

            DataGet dg = testEnv.getOperationFactory().newDataGet(txn1,data.getRowKey(),null);
            try(Partition p = testEnv.getPartition(DESTINATION_TABLE,tts)){
                DataResult result=p.get(dg,null);
                assertCorrectPresence(txn1,data,result);
            }
        }

        Txn txn2 = lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn2)){
            KVPair data=encode("scott3",null,30);
            callBuffer.add(data);
            try{
                callBuffer.flushBufferAndWait();
                Assert.fail("Did not throw a WriteConflict exception");
            }catch(ExecutionException ee){
                Throwable t=ee.getCause();
                t=testEnv.pipelineExceptionFactory().processPipelineException(t);
                Assert.assertTrue("Did not throw a Write conflict: instead: "+t,t instanceof WriteConflict);
            }
        }
    }

    @Test
    public void uniqueViolation() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn1 =lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn1)){
            KVPair data = encode("scott10",null,29);
            callBuffer.add(data);
            callBuffer.flushBufferAndWait();

            DataGet dg = testEnv.getOperationFactory().newDataGet(txn1,data.getRowKey(),null);
            try(Partition p = testEnv.getPartition(DESTINATION_TABLE,tts)){
                DataResult result=p.get(dg,null);
                assertCorrectPresence(txn1,data,result);
            }
        }finally{
            txn1.commit();
        }

        Txn txn2 = lifecycleManager.beginTransaction(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<KVPair> callBuffer=writeCoordinator.synchronousWriteBuffer(partitionFactory.getTable(DESTINATION_TABLE_BYTES),txn2)){
            KVPair data=encode("scott10",null,30);
            callBuffer.add(data);
            try{
                callBuffer.flushBufferAndWait();
                Assert.fail("Did not throw a UniqueConstraint violation");
            }catch(ExecutionException ee){
                Throwable t=ee.getCause();
                t=testEnv.pipelineExceptionFactory().processPipelineException(t);
                Assert.assertTrue("Did not throw a UniqueConstraint violation. instead: "+t,t instanceof UniqueConstraintViolation);
            }
        }
    }

    protected void assertCorrectPresence(Txn txn1,KVPair data,DataResult result){
        Assert.assertNotNull("Row was not written!");
        Assert.assertFalse("Incorrect number of cells returned!",result.isEmpty());
        Assert.assertNull("Returned a tombstone!",result.tombstoneOrAntiTombstone());
        DataCell dataCell=result.userData();
        Assert.assertNotNull("No User data written!");
        Assert.assertArrayEquals("Incorrect user data value!",data.getValue(),dataCell.value());
        Assert.assertEquals("Incorrect written timestamp!",txn1.getBeginTimestamp(),dataCell.version());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private KVPair encode(String name,String job,int age) throws IOException{
        BitSet setCols = new BitSet(3);
        BitSet scalarCols = new BitSet(3);
        BitSet empty = new BitSet();
        if(job!=null)
            setCols.set(1);
        if(age>=0){
            setCols.set(2);
            scalarCols.set(2);
        }

        EntryEncoder ee = EntryEncoder.create(kp,2,setCols,scalarCols,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        if(job!=null)
            entryEncoder.encodeNext(job);
        if(age>=0)
            entryEncoder.encodeNext(age);

        byte[] value=ee.encode();
        return new KVPair(Encoding.encode(name),value);
    }
}
