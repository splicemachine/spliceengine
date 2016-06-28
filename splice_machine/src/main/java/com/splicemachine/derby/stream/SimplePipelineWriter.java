package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;

import java.util.Iterator;

/**
 * Writer implementation which uses the Write Pipeline under the hood
 * Created by jyuan on 10/17/15.
 */
public class SimplePipelineWriter extends AbstractPipelineWriter<KVPair>{

    private long rowsWritten;
    private boolean skipIndex;

    public SimplePipelineWriter(TxnView txn,long heapConglom,boolean skipIndex){
        super(txn,heapConglom,null);
        this.skipIndex=skipIndex;
    }


    @Override
    public void open() throws StandardException{
        try{
            if(skipIndex){
                writeBuffer=writeCoordinator.noIndexWriteBuffer(destinationTable,txn,Metrics.noOpMetricFactory());
            }else{
                writeBuffer=writeCoordinator.writeBuffer(destinationTable,
                        txn,Metrics.noOpMetricFactory());
            }
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(KVPair kvPair) throws StandardException{
        try{
            writeBuffer.add(kvPair);
            rowsWritten++;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<KVPair> iterator) throws StandardException{
        while(iterator.hasNext()){
            write(iterator.next());
        }
    }

    public long getRowsWritten(){
        return rowsWritten;
    }
}
