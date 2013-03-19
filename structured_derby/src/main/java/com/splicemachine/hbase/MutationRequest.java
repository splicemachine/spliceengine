package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.Mutation;

import java.io.Externalizable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public abstract class MutationRequest implements Externalizable {
    private static final long serialVersionUID = 1l;
    protected List<Mutation> mutations = new ArrayList<Mutation>();


    public void addMutation(Mutation mutation){
        this.mutations.add(mutation);
    }

    public List<Mutation> getMutations(){
        return mutations;
    }
}
