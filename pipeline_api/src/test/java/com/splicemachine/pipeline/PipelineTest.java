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

package com.splicemachine.pipeline;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.TestTransactionSetup;
import com.splicemachine.storage.*;
import com.splicemachine.utils.IntArrays;
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
    private static TxnOperationFactory txnOperationFactory;


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
           txnOperationFactory = testEnv.getOperationFactory();
       }
    }

    @Test
    public void insertSingleRecord() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction();
        try (Partition p = partitionFactory.getTable(DESTINATION_TABLE_BYTES)) {
            try(RecordingCallBuffer<Record> callBuffer=writeCoordinator.synchronousWriteBuffer(p,txn)){
                Record record = txnOperationFactory.newRecord(txn,"scott1".getBytes());
                callBuffer.add(record);
                callBuffer.flushBufferAndWait();
                Record dg = p.get(record.getKey(), txn, IsolationLevel.SNAPSHOT_ISOLATION);
                assertCorrectPresence(txn, record, dg);
            }
        }finally{
            lifecycleManager.rollback(txn);
        }
    }

    @Test
    public void insertDeleteSingleRecord() throws Exception{
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction();
        try (Partition p = partitionFactory.getTable(DESTINATION_TABLE_BYTES)) {
            try (RecordingCallBuffer<Record> callBuffer = writeCoordinator.synchronousWriteBuffer(p, txn)) {
                Record record = insert(txn, "scott2", null, 29);
                callBuffer.add(record);
                callBuffer.flushBufferAndWait();
                Record result = p.get(record.getKey(), txn, IsolationLevel.SNAPSHOT_ISOLATION);
                assertCorrectPresence(txn, record, result);
                Record deleteRecord = txnOperationFactory.newDelete(txn, "scott2".getBytes());
                callBuffer.add(deleteRecord);
                callBuffer.flushBufferAndWait();
                Record fetchRecord = p.get("scott2".getBytes(),txn,IsolationLevel.SNAPSHOT_ISOLATION);
                Assert.assertTrue("Row was still found!", fetchRecord == null);
            }
        }finally{
            lifecycleManager.rollback(txn);
        }
    }

    @Test
    public void insertManyRecords() throws Exception{
        int numRecords = 100;
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction();
        Partition p = partitionFactory.getTable(DESTINATION_TABLE_BYTES);
        try(RecordingCallBuffer<Record> callBuffer=writeCoordinator.synchronousWriteBuffer(p,txn)){
            List<Record> data=new ArrayList<>(numRecords);
            for(int i=0;i<numRecords;i++){
                Record record=insert(txn,"ryan"+i,null,i);
                callBuffer.add(record);
                data.add(record);
            }
            callBuffer.flushBufferAndWait();

            //make sure that all the rows are present
            for(Record d : data){
                Record result = p.get(d.getKey(),txn, IsolationLevel.SNAPSHOT_ISOLATION);
                assertCorrectPresence(txn,d,result);
            }
        }finally{
            lifecycleManager.rollback(txn);
        }
    }

    @Test
    public void insertManyRecordsDeleteSome() throws Exception{
        int numRecords = 100;
        WriteCoordinator writeCoordinator=testEnv.writeCoordinator();
        PartitionFactory partitionFactory=writeCoordinator.getPartitionFactory();
        Txn txn =lifecycleManager.beginTransaction();
        try (Partition p = partitionFactory.getTable(DESTINATION_TABLE_BYTES)) {
            try(RecordingCallBuffer<Record> callBuffer=writeCoordinator.synchronousWriteBuffer(p,txn)) {
                List<Record> data = new ArrayList<>(numRecords);
                for (int i = 1000; i < 1000 + numRecords; i++) {
                    Record record = insert(txn, "ryan" + i, null, i);
                    callBuffer.add(record);
                    data.add(record);
                }
                callBuffer.flushBufferAndWait();
                Record result = null;
                for (Record d : data) {
                    result = p.get(txn, d.getKey(), result);
                    assertCorrectPresence(txn, d, result);
                }
                int i = 0;
                for (Record kvP : data) {
                    if (i % 2 == 0) {
                        Record deleteRecord = this.txnOperationFactory.newDelete(txn, (byte[]) kvP.getKey());
                        callBuffer.add(deleteRecord);
                    }
                    i++;
                }
                callBuffer.flushBufferAndWait();
                i = 0;
                for (Record d : data) {
                    result = p.get(d.getKey(), txn,IsolationLevel.SNAPSHOT_ISOLATION);
                    if (i % 2 == 0) {
                        Assert.assertTrue("Row was deleted, but still found!", result == null);
                    } else {
                        assertCorrectPresence(txn, d, result);
                    }
                    i++;
                }
            }finally{

            }

        }
    }
/*
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
*/
    protected void assertCorrectPresence(Txn txn1,Record data,Record result){

        /*
        Assert.assertNotNull("Row was not written!");
        Assert.assertTrue("Incorrect number of cells returned!",result.size()>0);
        Assert.assertFalse("Returned a tombstone!",result.hasTombstone());
        DataCell dataCell=result.userData();
        Assert.assertNotNull("No User data written!");
        Assert.assertArrayEquals("Incorrect user data value!",data.getValue(),dataCell.value());
        Assert.assertEquals("Incorrect written timestamp!",txn1.getBeginTimestamp(),dataCell.version());
        */
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private Record insert(Txn txn, String name,String job,int age) throws IOException{
        try {
            ExecRow row = new ValueRow(2);
            row.setRowArray(new DataValueDescriptor[]{new SQLVarchar(job), new SQLInteger(age)});
            return txnOperationFactory.newRecord(txn,name.getBytes(), IntArrays.count(2),row);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
 }
