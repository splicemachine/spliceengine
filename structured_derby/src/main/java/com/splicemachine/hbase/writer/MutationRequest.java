package com.splicemachine.hbase.writer;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public abstract class MutationRequest implements Externalizable {
    private static final long serialVersionUID = 1l;
    protected List<Mutation> mutations = new ArrayList<Mutation>();
    protected byte[] regionStartKey;

    protected MutationRequest(){}

    protected MutationRequest(byte[] regionStartKey) {
        this.regionStartKey = regionStartKey;
    }

    public void addMutation(Mutation mutation){
        this.mutations.add(mutation);
    }

    public byte[] getRegionStartKey(){
        return regionStartKey;
    }

    public List<Mutation> getMutations(){
        return mutations;
    }

    public void addAll(Collection<Mutation> mutations) {
        this.mutations.addAll(mutations);
    }

    @Override
    public String toString() {
        return "MutationRequest{"+mutations.size()+",regionStart="+ BytesUtil.toHex(regionStartKey)+"}";
    }
}
