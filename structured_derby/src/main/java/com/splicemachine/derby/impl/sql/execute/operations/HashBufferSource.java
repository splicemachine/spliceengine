package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.utils.HashUtils;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.datanucleus.sco.backed.Map;

import java.nio.ByteBuffer;

public class HashBufferSource{

    private static final Logger LOG = Logger.getLogger(HashBufferSource.class);
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
    private boolean bucketed;
    private byte[] hash;
    private int[] hashColumnIndexes;

    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, RowProviderIterator<ExecRow> sourceRows, HashMerger merger, KeyMarshall hasher, MultiFieldEncoder keyEncoder) throws StandardException {
        this(sinkRowPrefix, keyColumnIndexes, sourceRows, merger, hasher, keyEncoder, null, null);
    }

    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, RowProviderIterator<ExecRow> sourceRows, HashMerger merger, KeyMarshall hasher, MultiFieldEncoder keyEncoder, boolean[] sortOrder, AggregateFinisher finisher) throws StandardException {
        this(sinkRowPrefix, keyColumnIndexes, keyColumnIndexes, sourceRows, merger, hasher, keyEncoder, sortOrder, finisher, false);
    }
    public HashBufferSource(byte[] sinkRowPrefix, int[] keyColumnIndexes, int[] hashColumnIndexes, RowProviderIterator<ExecRow> sourceRows, HashMerger merger, KeyMarshall hasher, MultiFieldEncoder keyEncoder, boolean[] sortOrder, AggregateFinisher finisher, boolean bucketed) throws StandardException {
        this.hasher = hasher;
        this.keyEncoder = keyEncoder;
        this.bucketed = bucketed;
        if (bucketed) {
            this.hash = new byte[] {0};
            keyEncoder.setRawBytes(this.hash);
        }
        keyEncoder.setRawBytes(sinkRowPrefix);
        keyEncoder.mark();
        this.sourceRows = sourceRows;
        this.keyColumnIndexes = keyColumnIndexes;
        this.hashColumnIndexes = hashColumnIndexes;
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
                if (bucketed) {
                    updateHash();
                }
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

    private void updateHash() {
        if (!bucketed) {
            return;
        }
        byte[][] fields = new byte[hashColumnIndexes.length + 1][];
        // 0 is the hash byte
        // 1 is the UUID
        // 2 - N are the key fields
        for (int i = 0; i <= hashColumnIndexes.length; i++) {
            fields[i] = keyEncoder.getEncodedBytes(i + 1); // UUID            
        }
        this.hash[0] = HashUtils.hash(fields);
    }

    public void close() {
        if(keyEncoder!=null)
            keyEncoder.close();
    }
}
