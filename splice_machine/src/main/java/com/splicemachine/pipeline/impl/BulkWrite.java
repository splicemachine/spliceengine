package com.splicemachine.pipeline.impl;

import com.splicemachine.hbase.KVPair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public class BulkWrite {

    public Collection<KVPair> mutations;
    private String encodedStringName;
    private byte skipIndexWrite;

    /*non serialized field*/
    private transient long bufferHeapSize = -1;

    public BulkWrite(Collection<KVPair> mutations,String encodedStringName) {
        this(-1,mutations,encodedStringName);
    }

    public BulkWrite(Collection<KVPair> mutations, String encodedStringName, byte skipIndexWrite) {
        this(-1, mutations, encodedStringName);
        this.skipIndexWrite = skipIndexWrite;
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName) {
        assert encodedStringName != null;
        this.mutations = mutations;
        this.encodedStringName = encodedStringName;
        this.bufferHeapSize = heapSizeEstimate;
        this.skipIndexWrite = 0x02; // default to false - do not skip
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName, boolean skipIndexWrite) {
        this(heapSizeEstimate, mutations, encodedStringName);
        if (skipIndexWrite)
            this.skipIndexWrite = 0x01;  // true - skip writing to index
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
        return this.skipIndexWrite == 0x01;
    }

    public byte getSkipIndexWrite() {
        return this.skipIndexWrite;
    }
}
