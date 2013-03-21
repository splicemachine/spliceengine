package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public class UncompressedMutationRequest extends MutationRequest{
    private static final long serialVersionUID = 1l;

    public UncompressedMutationRequest() { }

    public UncompressedMutationRequest(byte[] regionStartKey) {
        super(regionStartKey);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(mutations.size());
        for(Mutation mutation:mutations){
            out.writeBoolean(mutation instanceof Put);
            ((Writable)mutation).write(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        mutations = new ArrayList<Mutation>(size);
        for(int i=0;i<size;i++){
            if(in.readBoolean()){
                Put put = new Put();
                put.readFields(in);
                mutations.add(put);
            }else{
                Delete delete = new Delete();
                delete.readFields(in);
                mutations.add(delete);
            }
        }
    }
}
