package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectPipelineWriter implements TableWriter<KVPair>,AutoCloseable{
    private final long destConglomerate;
    private TxnView txn;
    private final OperationContext opCtx;
    private final boolean skipIndex;

    private CallBuffer<KVPair> writeBuffer;

    public DirectPipelineWriter(long destConglomerate,TxnView txn,OperationContext opCtx,boolean skipIndex){
        this.destConglomerate=destConglomerate;
        this.txn=txn;
        this.opCtx=opCtx;
        this.skipIndex=skipIndex;
    }

    @Override
    public void open() throws StandardException{
        WriteCoordinator wc =PipelineDriver.driver().writeCoordinator();
        try{
            this.writeBuffer = wc.writeBuffer(Bytes.toBytes(Long.toString(destConglomerate)),txn,Metrics.noOpMetricFactory());
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void open(TriggerHandler triggerHandler,SpliceOperation dmlWriteOperation) throws StandardException{
        open();
    }

    @Override
    public void write(KVPair row) throws StandardException{
        try{
            writeBuffer.add(row);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<KVPair> rows) throws StandardException{
        while(rows.hasNext()){
            write(rows.next());
        }
    }

    @Override
    public void close() throws StandardException{
        try{
            writeBuffer.flushBufferAndWait();
            writeBuffer.close();
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void setTxn(TxnView txn){
        this.txn = txn;
    }

    @Override
    public TxnView getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(Long.toString(destConglomerate));
    }

    @Override
    public OperationContext getOperationContext() {
        return opCtx;
    }
}
