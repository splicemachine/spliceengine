/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.client;

import com.splicemachine.encoding.ExpandedDecoder;
import com.splicemachine.encoding.ExpandingEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import splice.com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Utilities around encoding and decoding BulkWriteRequests and responses.
 *
 * @author Scott Fines
 *         Date: 1/19/15
 */
public class PipelineEncoding {

    public static byte[] encode(TxnOperationFactory operationFactory,BulkWrites bulkWrites){
        /*
         * The encoding for a BulkWrites is as follows:
         * Txn (1-N bytes)
         * # of BulkWrites (1-N bytes)
         * for 1...# of BulkWrites:
         *  encodedStringName
         * for 1...# of BulkWrites:
         *  flags
         * for 1...# of BulkWrites:
         *  KVPairs
         *
         * This encoding follows the rule of "Header-body", where the "header" of the data
         * in this case is the metadata about the request, while the "body" is a byte array
         * sequence of KVPairs. This means that we can decode the necessary metadata eagerly,
         * but deserialize the KVPairs on an as-needed basis.
         *
         */
        byte[] txnBytes = operationFactory.encode(bulkWrites.getTxn());
        byte[] token = bulkWrites.getToken();
        if (token == null)
            token = new byte[0];

        int heapSize = bulkWrites.getBufferHeapSize();
        ExpandingEncoder buffer = new ExpandingEncoder(heapSize+txnBytes.length+token.length);
        buffer.rawEncode(txnBytes);
        buffer.rawEncode(token);

        //encode BulkWrite metadata
        Collection<BulkWrite> bws = bulkWrites.getBulkWrites();
        buffer.encode(bws.size());
        for(BulkWrite bw:bws){
            buffer.encode(bw.getEncodedStringName());
        }

        for(BulkWrite bw:bws){
            buffer.encode(bw.getFlags());
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


    public static BulkWrites decode(TxnOperationFactory operationFactory,byte[] data){
        ExpandedDecoder decoder = new ExpandedDecoder(data);
        byte[] txnBytes = decoder.rawBytes();
        byte[] token = decoder.rawBytes();
        TxnView txn = operationFactory.decode(txnBytes,0,txnBytes.length);
        int bwSize = decoder.decodeInt();
        List<String> stringNames = new ArrayList<>(bwSize);
        for(int i=0;i<bwSize;i++) {
            stringNames.add(decoder.decodeString());
        }
        byte[] flags = new byte[bwSize];
        for (int i=0; i<bwSize; i++) {
            flags[i] = decoder.decodeByte();
        }

        return new BulkWrites(new BulkWriteCol(flags,data,decoder.currentOffset(),stringNames),txn,null,token);
    }


    /***********************************************************************************************************/
    /*private helper classes*/
    private static class BulkWriteCol extends AbstractCollection<BulkWrite>{
        private final int kvOffset;
        private final List<String> encodedStringNames;
        private final byte[] flags;
        private final byte[] buffer;
        /*
         * we keep a cache of previously created BulkWrites, so that we can have
         * deterministic iteration (i.e. returning the same objects instead of
         * creating new with each iteration);
         */
        private transient Collection<BulkWrite> cache;
        private transient ExpandedDecoder decoder;
        private transient int lastIndex = 0;

        public BulkWriteCol(byte[] flags, byte[] buffer,int kvOffset, List<String> encodedStringNames) {
            this.kvOffset = kvOffset;
            this.encodedStringNames = encodedStringNames;
            this.buffer = buffer;
            this.flags = flags;
        }

        @Override
        @Nonnull
        public Iterator<BulkWrite> iterator() {
            if(cache!=null){
                if(cache.size()==encodedStringNames.size())
                    return cache.iterator();
                else{
                    /*
                     * We haven't read the entire data off yet, so we need to concatenate the
                     * cache with the remainder of the stuff
                     */
                    return Iterators.concat(cache.iterator(), new BulkIter(lastIndex));
                }
            }
            cache = new ArrayList<>(encodedStringNames.size());
            decoder = new ExpandedDecoder(buffer,kvOffset);
            return new BulkIter(0);
        }

        @Override public int size() { return encodedStringNames.size(); }

        private class BulkIter implements Iterator<BulkWrite> {
            final Iterator<String> encodedStrings;
            int index;

            public BulkIter(int startIndex) {
                this.index = startIndex;
                if(index!=0)
                    this.encodedStrings = encodedStringNames.subList(index,encodedStringNames.size()).iterator();
                else
                    this.encodedStrings = encodedStringNames.iterator();
            }

            @Override public boolean hasNext() { return encodedStrings.hasNext(); }
            @Override public void remove() { throw new UnsupportedOperationException(); }

            @Override
            public BulkWrite next() {
                String esN = encodedStrings.next();
                byte elementFlags = flags[index++];
                int size = decoder.decodeInt();
                Collection<KVPair> kvPairs = new ArrayList<>(size);
                KVPair template = new KVPair();
                ByteSlice rowKeySlice = template.rowKeySlice();
                ByteSlice valueSlice = template.valueSlice();
                for(int i=0;i<size;i++){
                    template.setType(KVPair.Type.decode(decoder.rawByte()));
                    decoder.sliceNext(rowKeySlice);
                    decoder.sliceNext(valueSlice);
                    kvPairs.add(template.shallowClone());
                }


                BulkWrite bulkWrite = new BulkWrite(kvPairs, esN, elementFlags);
                cache.add(bulkWrite);
                lastIndex=index;
                return bulkWrite;
            }
        }
    }
}
