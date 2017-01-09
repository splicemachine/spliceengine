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

import com.splicemachine.storage.Record;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite {

    public Collection<Record> mutations;
    private String encodedStringName;
    private byte skipIndexWrite;

    /*non serialized field*/
    private transient long bufferHeapSize = -1;

    public BulkWrite(Collection<Record> mutations,String encodedStringName) {
        this(-1,mutations,encodedStringName);
    }

    public BulkWrite(Collection<Record> mutations, String encodedStringName, byte skipIndexWrite) {
        this(-1, mutations, encodedStringName);
        this.skipIndexWrite = skipIndexWrite;
    }

    public BulkWrite(int heapSizeEstimate,Collection<Record> mutations,String encodedStringName) {
        assert encodedStringName != null;
        this.mutations = mutations;
        this.encodedStringName = encodedStringName;
        this.bufferHeapSize = heapSizeEstimate;
        this.skipIndexWrite = 0x02; // default to false - do not skip
    }

    public BulkWrite(int heapSizeEstimate,Collection<Record> mutations,String encodedStringName, boolean skipIndexWrite) {
        this(heapSizeEstimate, mutations, encodedStringName);
        if (skipIndexWrite)
            this.skipIndexWrite = 0x01;  // true - skip writing to index
    }

    public Collection<Record> getMutations() {
        return mutations;
    }

    public List<Record> mutationsList(){
        if(mutations instanceof List) return (List<Record>)mutations;
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
            for(Record kvPair:mutations){
                heap+=kvPair.getSize();
            }
            bufferHeapSize = heap;
        }
        return bufferHeapSize;
    }

    public int getSize() { return mutations.size(); }

    public boolean skipIndexWrite() {
        return this.skipIndexWrite == 0x01;
    }

    public byte getSkipIndexWrite() {
        return this.skipIndexWrite;
    }
}
