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

package com.splicemachine.derby.stream.spark;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;import com.splicemachine.derby.stream.ActivationHolder;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.util.Arrays;

/**
 * Encapsulates and ActivationHolder and the Broadcast Spark object used to transfer it to all servers.
 * Makes it possible to (de)serialize only once the activation and operation tree.
 *
 * Created by dgomezferro on 1/14/16.
 */
public class BroadcastedActivation implements Externalizable {
    private static ThreadLocal<ActivationHolderAndBytes> activationHolderTL =new ThreadLocal<>();
    private byte[] serializedValue;
    private ActivationHolder activationHolder;
    private Broadcast<byte[]> bcast;

    public BroadcastedActivation() {

    }

    public BroadcastedActivation (Activation activation, SpliceOperation root) {
        this.activationHolder = new ActivationHolder(activation, root);
        this.serializedValue = writeActivationHolder();
        this.bcast = SpliceSpark.getContext().broadcast(serializedValue);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bcast);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bcast = (Broadcast<byte[]>) in.readObject();
        serializedValue = bcast.getValue();
        ActivationHolderAndBytes ah= activationHolderTL.get();
        if(ah==null || !Arrays.equals(ah.bytes,serializedValue)){
            ah = readActivationHolder();
            activationHolderTL.set(ah);
        }

        activationHolder = ah.activationHolder;
    }

    public ActivationHolder getActivationHolder() {
        return activationHolder;
    }

    private byte[] writeActivationHolder(){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(ObjectOutputStream oos = new ObjectOutputStream(baos)){
            oos.writeObject(activationHolder);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    public ActivationHolderAndBytes readActivationHolder(){
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedValue);
        try(ObjectInputStream ois = new ObjectInputStream(bais)){
            return new ActivationHolderAndBytes((ActivationHolder)ois.readObject(),serializedValue);
        }catch(ClassNotFoundException | IOException e){
            throw new RuntimeException(e);
        }
    }

    public static class ActivationHolderAndBytes {
        ActivationHolder activationHolder;
        byte[] bytes;

        public ActivationHolderAndBytes(ActivationHolder activationHolder,byte[] bytes){
            this.activationHolder=activationHolder;
            this.bytes=bytes;
        }

        public ActivationHolder getActivationHolder() {
            return activationHolder;
        }
    }

    public ActivationHolderAndBytes getActivationHolderAndBytes() {
        return activationHolderTL.get();
    }

    public void setActivationHolder(ActivationHolder ah) {
        activationHolder = ah;
    }
}