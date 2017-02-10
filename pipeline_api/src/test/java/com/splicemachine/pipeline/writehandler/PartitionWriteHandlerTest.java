/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.concurrent.ResettableCountDownLatch;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.testsetup.PipelineTestDataEnv;
import com.splicemachine.pipeline.testsetup.PipelineTestEnvironment;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.si.impl.store.TestingTimestampSource;
import com.splicemachine.si.impl.store.TestingTxnStore;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import com.splicemachine.si.testenv.TestServerControl;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 2/25/16
 */
@Category(ArchitectureSpecific.class)
public class PartitionWriteHandlerTest{
    private static final KryoPool kp=new KryoPool(1);
    private TxnSupplier testStore;
    private OperationFactory opFactory;
    private OperationStatusFactory opStatusFactory;
    private ExceptionFactory ef;
    private PipelineExceptionFactory pef;

    @Before
    public void setUp() throws Exception{
        PipelineTestDataEnv dataEnv =PipelineTestEnvironment.loadTestDataEnvironment();
        ef = dataEnv.getExceptionFactory();
        pef = dataEnv.pipelineExceptionFactory();
        testStore = new TestingTxnStore(new IncrementingClock(),new TestingTimestampSource(),ef, Long.MAX_VALUE);
        opFactory = dataEnv.getBaseOperationFactory();
        opStatusFactory = dataEnv.getOperationStatusFactory();
    }

    @Test
    public void testDoesNotWriteDataWhenRegionCloses() throws Exception{
        ServerControl sc = new TestServerControl();

        Partition p = mock(Partition.class);
        when(p.getName()).thenReturn("testRegion");
        when(p.containsRow(any(byte[].class),anyInt(),anyInt())).thenReturn(Boolean.TRUE);
        when(p.getRowLock(any(byte[].class),anyInt(),anyInt())).thenReturn(new ReentrantLock());
        doNothing().when(p).startOperation();

        DataAnswer dataAnswer=new DataAnswer(opStatusFactory);
        NotServingRegionAnswer<Iterator<MutationStatus>> iteratorNotServingRegionAnswer=new NotServingRegionAnswer<>(p.getName(),ef);
        when(p.writeBatch(any(DataPut[].class))).then(new LatchAnswer<>(Arrays.asList(iteratorNotServingRegionAnswer,dataAnswer)));
        PartitionFactory pf = mock(PartitionFactory.class);
        TxnOperationFactory txnOpFactory = new SimpleTxnOperationFactory(ef,opFactory);
        TransactionalRegion testRegion = new TxnRegion(p,NoopRollForward.INSTANCE,NoOpReadResolver.INSTANCE,testStore,
                new SITransactor(testStore,txnOpFactory,opFactory,opStatusFactory,ef),
                txnOpFactory);


        WriteContext writeContext = mock(WriteContext.class);
        when(writeContext.getRegion()).thenReturn(p);
        when(writeContext.txnRegion()).thenReturn(testRegion);
        when(writeContext.getTxn()).thenReturn(new ActiveWriteTxn(1l,1l,Txn.ROOT_TRANSACTION,true,Txn.IsolationLevel.SNAPSHOT_ISOLATION));
        when(writeContext.getCoprocessorEnvironment()).thenReturn(sc);
        when(writeContext.exceptionFactory()).thenReturn(pef);
        when(writeContext.canRun(any(KVPair.class))).thenReturn(Boolean.TRUE);
        final HashMap<KVPair, WriteResult> resultsMap=new HashMap<>();
        when(writeContext.currentResults()).thenReturn(resultsMap);
        Answer<Void> resultAnswer=new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable{
                KVPair p=(KVPair)invocation.getArguments()[0];
                WriteResult wr=(WriteResult)invocation.getArguments()[1];
                resultsMap.put(p,wr);
                return null;
            }
        };
        doAnswer(resultAnswer).when(writeContext).failed(any(KVPair.class),any(WriteResult.class));
        doAnswer(resultAnswer).when(writeContext).result(any(KVPair.class),any(WriteResult.class));

        ResettableCountDownLatch writeLatch=new ResettableCountDownLatch(1);
        writeLatch.countDown();
        PartitionWriteHandler pwh = new PartitionWriteHandler(testRegion,writeLatch,null);

        Collection<KVPair> data = addData(100,2);
        for(KVPair d:data){
            pwh.next(d,writeContext);
        }
        boolean threwError = false;
        try{
            pwh.flush(writeContext);
            pwh.close(writeContext);
        }catch(IOException ioe){
            Assert.assertTrue("Incorrect error message: "+ioe,ioe instanceof NotServingPartitionException);
            threwError=true;
        }
        Assert.assertEquals("Data was written, even though an NSPE was thrown!",0,dataAnswer.data.size());
        if(!threwError){
            /*
             * A flush is not required to throw the error directly. It is ALSO allowed to return
             * error codes on all data elements, so we check for those if we don't get an error.
             */
            for(KVPair d : data){
                WriteResult wr=resultsMap.get(d);
                Assert.assertNotNull("No WriteResponse for row!",wr);
                Assert.assertEquals("Incorrect ResultCode!",WriteResult.notServingRegion().getCode(),wr.getCode());
            }
        }else{
            /*
             * Because we threw the error, the return code is allowed to either be null OR
             * to contain NotServingRegion
             */
            for(KVPair d : data){
                WriteResult wr=resultsMap.get(d);
                if(wr!=null){
                    Assert.assertEquals("Incorrect ResultCode!",WriteResult.notServingRegion().getCode(),wr.getCode());
                }
            }
        }

    }

    private Collection<KVPair> addData(int startPoint,int size) throws IOException{
        Collection<KVPair> data=new ArrayList<>(size);
        for(int i=startPoint;i<startPoint+size;i++){
            KVPair kvP=encode("ryan"+i,null,i);
            data.add(kvP);
        }
        return data;
    }

    private KVPair encode(String name,String job,int age) throws IOException{
        BitSet setCols=new BitSet(3);
        BitSet scalarCols=new BitSet(3);
        BitSet empty=new BitSet();
        if(job!=null)
            setCols.set(1);
        if(age>=0){
            setCols.set(2);
            scalarCols.set(2);
        }

        EntryEncoder ee=EntryEncoder.create(kp,2,setCols,scalarCols,empty,empty);
        MultiFieldEncoder entryEncoder=ee.getEntryEncoder();
        if(job!=null)
            entryEncoder.encodeNext(job);
        if(age>=0)
            entryEncoder.encodeNext(age);

        byte[] value=ee.encode();
        return new KVPair(Encoding.encode(name),value);
    }

    private static class LatchAnswer<T> implements Answer<T>{
        int count = 0;
        final int maxCount;
        final Map<Integer,Answer<T>> stageMap = new HashMap<>();

        private LatchAnswer(List<Answer<T>> answers){
            this.maxCount =answers.size();

            for(int i=0;i<answers.size();i++){
                stageMap.put(i,answers.get(i));
            }
        }

        @Override
        public T answer(InvocationOnMock invocation) throws Throwable{
            Assert.assertTrue("Called too many times!",count<maxCount);
            Answer<T> stageAnswer = stageMap.get(count);
            Assert.assertNotNull("No Stage answer found!",stageAnswer);
            T ret = stageAnswer.answer(invocation);
            count++;
            return ret;
        }
    }

    private static class NoopAnswer implements Answer<Void>{
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable{
            return null;
        }
    }

    private static class DataAnswer implements Answer<Iterator<MutationStatus>>{
        final OperationStatusFactory opStatusFactory;
        final List<DataPut> data;

        public DataAnswer(OperationStatusFactory opStatusFactory){
            this.opStatusFactory=opStatusFactory;
            this.data = new ArrayList<>();
        }

        @Override
        public Iterator<MutationStatus> answer(InvocationOnMock invocation) throws Throwable{
            DataPut[] dp = (DataPut[])invocation.getArguments()[0];
            Assert.assertNotNull("Null data puts!",dp);
            Collections.addAll(data,dp);
            List<MutationStatus> success = new ArrayList<>(dp.length);
            MutationStatus s = opStatusFactory.success();
            for(int i=0;i<dp.length;i++){
                success.add(s);
            }
            return success.iterator();
        }
    }

    private static class NotServingRegionAnswer<T> implements Answer<T>{
        private final ExceptionFactory ef;
        private String partitionName;

        public NotServingRegionAnswer(String partitionName,ExceptionFactory ef){
            this.ef=ef;
            this.partitionName = partitionName;
        }

        @Override
        public T answer(InvocationOnMock invocation) throws Throwable{
            throw ef.notServingPartition(partitionName);
        }
    }
}