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

    /*non serialized field*/
    private transient long bufferHeapSize = -1;

    public BulkWrite(Collection<KVPair> mutations,String encodedStringName) {
        this(-1,mutations,encodedStringName);
    }

    public BulkWrite(int heapSizeEstimate,Collection<KVPair> mutations,String encodedStringName) {
        assert encodedStringName != null;
        this.mutations = mutations;
        this.encodedStringName = encodedStringName;
        this.bufferHeapSize = heapSizeEstimate;
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
}
