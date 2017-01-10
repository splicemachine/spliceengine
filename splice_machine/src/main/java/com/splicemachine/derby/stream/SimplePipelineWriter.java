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

package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import java.util.Iterator;

/**
 * Writer implementation which uses the Write Pipeline under the hood
 * Created by jyuan on 10/17/15.
 */
public class SimplePipelineWriter extends AbstractPipelineWriter<Record>{

    private long rowsWritten;
    private boolean skipIndex;

    public SimplePipelineWriter(Txn txn, long heapConglom, boolean skipIndex){
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
    public void write(Record kvPair) throws StandardException{
        try{
            writeBuffer.add(kvPair);
            rowsWritten++;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void write(Iterator<Record> iterator) throws StandardException{
        while(iterator.hasNext()){
            write(iterator.next());
        }
    }

    public long getRowsWritten(){
        return rowsWritten;
    }
}
