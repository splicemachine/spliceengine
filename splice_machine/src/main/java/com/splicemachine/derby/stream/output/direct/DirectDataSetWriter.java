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

package com.splicemachine.derby.stream.output.direct;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.control.ControlPairDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/13/16
 */
public class DirectDataSetWriter<K> implements DataSetWriter{
    private final ControlPairDataSet<K,Record> dataSet;
    private final DirectPipelineWriter pipelineWriter;

    public DirectDataSetWriter(ControlPairDataSet<K, Record> dataSet,
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
            pipelineWriter.close(); //make sure everything gets written

            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(rows.count));
            return new ControlDataSet<>(new SingletonIterator(new LocatedRow(valueRow)));
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void setTxn(Txn childTxn){
        pipelineWriter.setTxn(childTxn);
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return pipelineWriter;
    }

    @Override
    public Txn getTxn(){
        return pipelineWriter.getTxn();
    }

    @Override
    public byte[] getDestinationTable(){
        return pipelineWriter.getDestinationTable();
    }

    private class CountingIterator implements Iterator<Record>{
        private long count = 0;
        private Iterator<Record> delegate;

        public CountingIterator(Iterator<Record> delegate){
            this.delegate=delegate;
        }

        @Override
        public boolean hasNext(){
            return delegate.hasNext();
        }

        @Override
        public Record next(){
            Record n = delegate.next();
            count++;
            return n;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException();
        }
    }
}
