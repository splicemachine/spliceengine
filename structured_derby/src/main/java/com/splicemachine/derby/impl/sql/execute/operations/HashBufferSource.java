package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Pair;
import org.datanucleus.sco.backed.Map;

import java.nio.ByteBuffer;

public class HashBufferSource{

    private byte[] keyBytes;
    private KeyMarshall hasher;
    protected MultiFieldEncoder keyEncoder;
    private HashBuffer<ByteBuffer,ExecRow> currentRows = new HashBuffer<ByteBuffer,ExecRow>(SpliceConstants.ringBufferSize);
    private boolean doneReadingSource = false;
    private RowProviderIterator<ExecRow> sourceRows;
    private int[] keyColumnIndexes;
    private HashMerger merger;
    private AggregateFinisher<ByteBuffer,ExecRow> finisher;
    private boolean[] sortOrder;

    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, RowProviderIterator<ExecRow> sourceRows, HashMerger merger, KeyMarshall hasher, MultiFieldEncoder keyEncoder) throws StandardException {
        this(sinkRowPrefix, keyColumnIndexes, sourceRows, merger, hasher, keyEncoder, null, null);
    }

    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, RowProviderIterator<ExecRow> sourceRows, HashMerger merger, KeyMarshall hasher, MultiFieldEncoder keyEncoder, boolean[] sortOrder, AggregateFinisher finisher) throws StandardException {
        this.hasher = hasher;
        this.keyEncoder = keyEncoder;
        keyEncoder.setRawBytes(sinkRowPrefix);
        keyEncoder.mark();
        this.sourceRows = sourceRows;
        this.keyColumnIndexes = keyColumnIndexes;
        this.merger = merger;
        this.finisher = finisher;
        this.sortOrder = sortOrder;
    }

    public Pair<ByteBuffer, ExecRow> getNextAggregatedRow() throws StandardException {
        Pair<ByteBuffer, ExecRow> result = null;

        if(!doneReadingSource){
            while(sourceRows.hasNext() && result == null){
                ExecRow nextRow = sourceRows.next();
                keyEncoder.reset();
                hasher.encodeKey(nextRow.getRowArray(),keyColumnIndexes,sortOrder,null,keyEncoder);
                keyBytes = keyEncoder.build();
                ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
                if (!currentRows.merge(buffer, nextRow, merger)) {
                    Map.Entry<ByteBuffer,ExecRow> finalized = currentRows.add(buffer,nextRow.getClone());

                    if(finalized!=null&&finalized!=nextRow){
                        result = new Pair(finalized.getKey(), finalized.getValue());

                        if(finisher != null){
                            finisher.finishAggregation(result.getSecond());
                        }
                        return result;
                    }
                }
            }
            doneReadingSource = result==null;

            if(finisher != null){
                currentRows = currentRows.finishAggregates(finisher);
            }
        }

        if(doneReadingSource && !currentRows.isEmpty()){
            ByteBuffer key = currentRows.keySet().iterator().next();
            result = new Pair(key, currentRows.remove(key));
        }

        return result;
    }

    public void close() {
        if(keyEncoder!=null)
            keyEncoder.close();
    }
}
