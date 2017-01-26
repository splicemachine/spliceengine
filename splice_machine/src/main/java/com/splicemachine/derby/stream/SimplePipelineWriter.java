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
