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
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.commons.collections.iterators.SingletonIterator;

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
            pipelineWriter.close(); //make sure everything gets written

            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(rows.count));
            return new ControlDataSet<>(new SingletonIterator(new LocatedRow(valueRow)));
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void setTxn(TxnView childTxn){
        pipelineWriter.setTxn(childTxn);
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return pipelineWriter;
    }

    @Override
    public TxnView getTxn(){
        return pipelineWriter.getTxn();
    }

    @Override
    public byte[] getDestinationTable(){
        return pipelineWriter.getDestinationTable();
    }

    private class CountingIterator implements Iterator<KVPair>{
        private long count = 0;
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
