package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.control.ControlPairDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectDataSetWriter<K> implements DataSetWriter{
    private final ControlPairDataSet<K,KVPair> dataSet;
    private final DirectPipelineWriter pipelineWriter;

    public DirectDataSetWriter(ControlPairDataSet<K, KVPair> dataSet,
                               DirectPipelineWriter pipelineWriter){
        this.dataSet=dataSet;
        this.pipelineWriter=pipelineWriter;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        try{
            pipelineWriter.open();
            CountingIterator rows=new CountingIterator(dataSet.values().toLocalIterator());
            pipelineWriter.write(rows);

            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger(rows.count));
            return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    private class CountingIterator implements Iterator<KVPair>{
        private int count = 0;
        private Iterator<KVPair> delegate;

        public CountingIterator(Iterator<KVPair> delegate){
            this.delegate=delegate;
        }

        @Override
        public boolean hasNext(){
            return delegate.hasNext();
        }

        @Override
        public KVPair next(){
            KVPair n = delegate.next();
            count++;
            return n;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException();
        }
    }
}
