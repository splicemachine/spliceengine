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

package com.splicemachine.pipeline.client;

import com.splicemachine.kvpair.KVPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite {

    enum Flags {
        SKIP_INDEX_WRITE((byte)0x01),
        SKIP_CONFLICT_DETECTION((byte)0x02),
        SKIP_WAL((byte)0x04);

        final byte mask;
        Flags(byte mask) {
            this.mask = mask;
        }

        boolean isSetIn(byte flags) {
            return (flags & mask) != 0;
        }
    }

    public Collection<KVPair> mutations;
    private String encodedStringName;
    private boolean skipIndexWrite;
    private boolean skipConflictDetection;
    private boolean skipWAL;

    /*non serialized field*/
    private transient long bufferHeapSize = -1;

    public BulkWrite(Collection<KVPair> mutations,String encodedStringName) {
        this(-1,mutations,encodedStringName);
    }

    public BulkWrite(Collection<KVPair> mutations, String encodedStringName, byte flags) {
        this(-1, mutations, encodedStringName);
        this.skipIndexWrite = Flags.SKIP_INDEX_WRITE.isSetIn(flags);
        this.skipConflictDetection = Flags.SKIP_CONFLICT_DETECTION.isSetIn(flags);
        this.skipWAL = Flags.SKIP_WAL.isSetIn(flags);
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName) {
        assert encodedStringName != null;
        this.mutations = mutations;
        this.encodedStringName = encodedStringName;
        this.bufferHeapSize = heapSizeEstimate;
        this.skipIndexWrite = false; // default to false - do not skip
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName,
                     boolean skipIndexWrite, boolean skipConflictDetection, boolean skipWAL) {
        this(heapSizeEstimate, mutations, encodedStringName);
        this.skipIndexWrite = skipIndexWrite;
        this.skipConflictDetection = skipConflictDetection;
        this.skipWAL = skipWAL;
    }

    public Collection<KVPair> getMutations() {
        return mutations;
    }

    public List<KVPair> mutationsList(){
        if(mutations instanceof List) return (List<KVPair>)mutations;
        return new ArrayList<>(mutations);
    }

    public String getEncodedStringName() {
        return encodedStringName;
    }

    @Override
    public String toString() {
        return "BulkWrite{" +
                ", encodedStringName='" + encodedStringName + '\'' +
                ", rows="+mutations.size()+
                '}';
    }

    public long getBufferSize() {
        if(bufferHeapSize <0){
            long heap = 0l;
            for(KVPair kvPair:mutations){
                heap+=kvPair.getSize();
            }
            bufferHeapSize = heap;
        }
        return bufferHeapSize;
    }

    public int getSize() { return mutations.size(); }

    public boolean skipIndexWrite() {
        return this.skipIndexWrite;
    }

    public byte getFlags() {
        byte result = 0;
        if (skipIndexWrite)
            result |= Flags.SKIP_INDEX_WRITE.mask;
        if (skipConflictDetection)
            result |= Flags.SKIP_CONFLICT_DETECTION.mask;
        if (skipWAL)
            result |= Flags.SKIP_WAL.mask;
        return result;
    }

    public boolean skipConflictDetection() {
        return skipConflictDetection;
    }

    public boolean skipWAL() { return skipWAL; }
}
