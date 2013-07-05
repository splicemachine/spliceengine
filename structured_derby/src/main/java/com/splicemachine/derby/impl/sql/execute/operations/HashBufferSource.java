package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.datanucleus.sco.backed.Map;

import java.nio.ByteBuffer;

public class HashBufferSource {

    private byte[] keyBytes;
    private KeyMarshall hasher;
    protected MultiFieldEncoder keyEncoder;
    private HashBuffer<ByteBuffer,ExecRow> currentRows = new HashBuffer<ByteBuffer,ExecRow>(SpliceConstants.ringBufferSize);
    private boolean doneReadingSource = false;
    private RowProviderIterator<ExecRow> sourceRows;
    private int[] keyColumnIndexes;

    private final HashBuffer.Merger<ByteBuffer,ExecRow> merger = new HashBuffer.Merger<ByteBuffer,ExecRow>() {
        @Override
        public ExecRow shouldMerge(ByteBuffer key){
            return currentRows.get(key);
        }

        @Override
        public void merge(ExecRow curr,ExecRow next){
            //throw away the second row, since we only want to keep the first
            //this is effectively a no-op
        }
    };


    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, RowProviderIterator<ExecRow> sourceRows) throws StandardException {
        hasher = KeyType.FIXED_PREFIX;
        keyEncoder = MultiFieldEncoder.create(keyColumnIndexes.length+1);
        keyEncoder.setRawBytes(sinkRowPrefix);
        keyEncoder.mark();
        this.sourceRows = sourceRows;
        this.keyColumnIndexes = keyColumnIndexes;

    }

    public ExecRow getNextAggregatedRow() throws StandardException {

        ExecRow resultRow = null;

        if(!doneReadingSource){
            while(sourceRows.hasNext() && resultRow == null){
                ExecRow nextRow = sourceRows.next();
                keyEncoder.reset();
                hasher.encodeKey(nextRow.getRowArray(),keyColumnIndexes,null,null,keyEncoder);
                keyBytes = keyEncoder.build();
                ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
                if (!currentRows.merge(buffer, nextRow, merger)) {
                    Map.Entry<ByteBuffer,ExecRow> finalized = currentRows.add(buffer,nextRow.getClone());

                    if(finalized!=null&&finalized!=nextRow){
                        resultRow = finalized.getValue();
                    }
                }
            }
            doneReadingSource = true;
        }

        if(resultRow == null && doneReadingSource && !currentRows.isEmpty()){
            ByteBuffer key = currentRows.keySet().iterator().next();
            resultRow = currentRows.remove(key);
        }

        return resultRow;
    }
}
