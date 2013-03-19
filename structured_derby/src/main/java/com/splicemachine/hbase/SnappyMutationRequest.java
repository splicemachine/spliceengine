package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Writable;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.ArrayList;

/**
 * @author Scott Fines
 *         Created on: 3/18/13
 */
public class SnappyMutationRequest  extends MutationRequest {
    private static final long serialVerionUID = 1l;

    public SnappyMutationRequest() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] bytes = toBytes();

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] bytes = new byte[in.readInt()];
        in.read(bytes);

        fromBytes(bytes);
    }

     private byte[] toBytes() throws IOException {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream output = new DataOutputStream(baos);
         output.writeInt(mutations.size());
         for(Mutation mutation:mutations){
             output.writeBoolean(mutation instanceof Put);
             ((Writable)mutation).write(output);
         }
         output.flush();
         output.close();
         return Snappy.compress(baos.toByteArray());
     }

    private void fromBytes(byte[] bytes) throws IOException {
        bytes = Snappy.uncompress(bytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInput input = new DataInputStream(bais);
        int inputSize = input.readInt();
        mutations = new ArrayList<Mutation>(inputSize);
        for(int i=0;i<inputSize;i++){
            if(input.readBoolean()){
                Put put = new Put();
                put.readFields(input);
                mutations.add(put);
            }else{
                Delete delete = new Delete();
                delete.readFields(input);
                mutations.add(delete);
            }
        }
    }
}
