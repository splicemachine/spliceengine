package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import org.apache.spark.broadcast.Broadcast;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Encapsulates an ActivationHolder and the Broadcast Spark object used to transfer it to all servers.
 * Makes it possible to (de)serialize only once the activation and operation tree
 *
 * Created by dgomezferro on 1/14/16.
 */
public class BroadcastedActivation implements Externalizable {
    private static ThreadLocal<ActivationHolderAndBytes> activationHolderTL = new ThreadLocal();
    private byte[] serializedValue;
    private ActivationHolder activationHolder;
    private Broadcast<byte[]> bcast;

    public BroadcastedActivation() {

    }

    public BroadcastedActivation (Activation activation) {
        this.activationHolder = new ActivationHolder(activation);
        this.serializedValue = writeActivationHolder();
        this.bcast = SpliceSpark.getContext().broadcast(serializedValue);
    }

    private byte[] writeActivationHolder() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(activationHolder);
            oos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bcast);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bcast = (Broadcast<byte[]>) in.readObject();
        serializedValue = bcast.getValue();
        ActivationHolderAndBytes ah = activationHolderTL.get();
        if (ah == null || !Arrays.equals(ah.bytes, serializedValue)) {
            ah = readActivationHolder();
            activationHolderTL.set(ah);
        }
        activationHolder = ah.activationHolder;
    }

    private ActivationHolderAndBytes readActivationHolder() {
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(serializedValue));
            return new ActivationHolderAndBytes((ActivationHolder) ois.readObject(), serializedValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                ois.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public ActivationHolder getActivationHolder() {
        return activationHolder;
    }

    private static class ActivationHolderAndBytes {
        ActivationHolder activationHolder;
        byte[] bytes;

        public ActivationHolderAndBytes(ActivationHolder activationHolder, byte[] bytes) {
            this.activationHolder = activationHolder;
            this.bytes = bytes;
        }
    }
}

