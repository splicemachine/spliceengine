package com.splicemachine.pipeline.impl;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.ExpandedDecoder;
import com.splicemachine.encoding.ExpandingEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnView;

import java.util.*;

/**
 * Utilities around encoding and decoding BulkWriteRequests and responses.
 *
 * @author Scott Fines
 *         Date: 1/19/15
 */
public class PipelineEncoding {

    public static byte[] encode(BulkWrites bulkWrites){
        /*
         * The encoding for a BulkWrites is as follows:
         * Txn (1-N bytes)
         * # of BulkWrites (1-N bytes)
         * for 1...# of BulkWrites:
         *  encodedStringName
         * for 1...# of BulkWrites:
         *  KVPairs
         *
         * This encoding follows the rule of "Header-body", where the "header" of the data
         * in this case is the metadata about the request, while the "body" is a byte array
         * sequence of KVPairs. This means that we can decode the necessary metadata eagerly,
         * but deserialize the KVPairs on an as-needed basis.
         *
         */
        byte[] txnBytes = TransactionOperations.getOperationFactory().encode(bulkWrites.getTxn());

        int heapSize = bulkWrites.getBufferHeapSize();
        ExpandingEncoder buffer = new ExpandingEncoder(heapSize+txnBytes.length);
        buffer.rawEncode(txnBytes);

        //encode BulkWrite metadata
        Collection<BulkWrite> bws = bulkWrites.getBulkWrites();
        buffer.encode(bws.size());
        for(BulkWrite bw:bws){
            buffer.encode(bw.getEncodedStringName());
        }

        for(BulkWrite bw:bws){
            Collection<KVPair> mutations = bw.getMutations();
            buffer.encode(mutations.size());
            for(KVPair kvPair:mutations){
                //TODO -sf- use a run-length encoding for type information here?
                buffer.rawEncode(kvPair.getType().asByte());
                buffer.rawEncode(kvPair.rowKeySlice());
                buffer.rawEncode(kvPair.valueSlice());
            }
        }
        return buffer.getBuffer();
    }


    public static BulkWrites decode(byte[] data){
        ExpandedDecoder decoder = new ExpandedDecoder(data);
        byte[] txnBytes = decoder.rawBytes();
        TxnView txn = TransactionOperations.getOperationFactory().decode(txnBytes,0,txnBytes.length);
        int bwSize = decoder.decodeInt();
        Collection<String> stringNames = new ArrayList<>(bwSize);
        for(int i=0;i<bwSize;i++) {
            stringNames.add(decoder.decodeString());
        }

        return new BulkWrites(new BulkWriteCol(data,decoder.currentOffset(),stringNames),txn);
    }


    /***********************************************************************************************************/
    /*private helper classes*/
    private static class BulkWriteCol extends AbstractCollection<BulkWrite>{
        private final int kvOffset;
        private final Collection<String> encodedStringNames;
        private final byte[] buffer;
        /*
         * we keep a cache of previously created BulkWrites, so that we can have
         * deterministic iteration (i.e. returning the same objects instead of
         * creating new with each iteration);
         */
        private transient Collection<BulkWrite> cache;

        public BulkWriteCol(byte[] buffer,int kvOffset, Collection<String> encodedStringNames) {
            this.kvOffset = kvOffset;
            this.encodedStringNames = encodedStringNames;
            this.buffer = buffer;
        }

        @Override
        public Iterator<BulkWrite> iterator() {
            if(cache!=null) return cache.iterator();
            cache = new ArrayList<>(encodedStringNames.size());
            return new BulkIter(kvOffset);
        }

        @Override public int size() { return encodedStringNames.size(); }

        private class BulkIter implements Iterator<BulkWrite> {
            final Iterator<String> encodedStrings = encodedStringNames.iterator();
            int iterOffset;
            final ExpandedDecoder decoder;

            public BulkIter(int iterOffset) {
                this.iterOffset = iterOffset;
                this.decoder = new ExpandedDecoder(buffer,iterOffset);
            }

            @Override public boolean hasNext() { return encodedStrings.hasNext(); }
            @Override public void remove() { throw new UnsupportedOperationException(); }

            @Override
            public BulkWrite next() {
                String esN = encodedStrings.next();
                int size = decoder.decodeInt();
                Collection<KVPair> kvPairs = new ArrayList<>(size);
                KVPair template = new KVPair();
                for(int i=0;i<size;i++){
                    template.setType(KVPair.Type.decode(decoder.rawByte()));
                    decoder.sliceNext(template.rowKeySlice());
                    decoder.sliceNext(template.valueSlice());
                    kvPairs.add(template.shallowClone());
                }


                BulkWrite bulkWrite = new BulkWrite(kvPairs, esN);
                cache.add(bulkWrite);
                return bulkWrite;
            }
        }
    }
}
