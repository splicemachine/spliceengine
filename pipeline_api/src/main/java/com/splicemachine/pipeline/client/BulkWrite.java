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

import com.splicemachine.kvpair.KVPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite {

    enum Flags {
        SKIP_INDEX_WRITE((byte)0x01),
        SKIP_CONFLICT_DETECTION((byte)0x02),
        SKIP_WAL((byte)0x04),
        ROLL_FORWARD((byte)0x08);

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
    private boolean rollforward;

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
        this.rollforward = Flags.ROLL_FORWARD.isSetIn(flags);
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName) {
        assert encodedStringName != null;
        this.mutations = mutations;
        this.encodedStringName = encodedStringName;
        this.bufferHeapSize = heapSizeEstimate;
        this.skipIndexWrite = false; // default to false - do not skip
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName,
                     boolean skipIndexWrite, boolean skipConflictDetection, boolean skipWAL, boolean rollforward) {
        this(heapSizeEstimate, mutations, encodedStringName);
        this.skipIndexWrite = skipIndexWrite;
        this.skipConflictDetection = skipConflictDetection;
        this.skipWAL = skipWAL;
        this.rollforward = rollforward;
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
                ", rows="+mutations+
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
        if (rollforward)
            result |= Flags.ROLL_FORWARD.mask;
        return result;
    }

    public boolean skipConflictDetection() {
        return skipConflictDetection;
    }

    public boolean skipWAL() { return skipWAL; }

    public boolean isRollforward() {
        return rollforward;
    }

    public void addTypes(Set<KVPair.Type> types) {
        for (KVPair kvPair : mutations) {
            types.add(kvPair.getType());
        }
    }
}
