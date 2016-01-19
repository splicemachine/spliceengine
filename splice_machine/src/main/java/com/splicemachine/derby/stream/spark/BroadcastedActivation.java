package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import org.apache.spark.broadcast.Broadcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Encapsulates an ActivationHolder and the Broadcast Spark object used to transfer it to all servers.
 * Makes it possible to (de)serialize only once the activation and operation tree
 *
 * Created by dgomezferro on 1/14/16.
 */
public class BroadcastedActivation implements Externalizable {
    private ActivationHolder activationHolder;
    private Broadcast<ActivationHolder> bcast;

    public BroadcastedActivation() {

    }

    public BroadcastedActivation (Activation activation) {
        this.activationHolder = new ActivationHolder(activation);
        this.bcast = SpliceSpark.getContext().broadcast(activationHolder);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(bcast);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        bcast = (Broadcast<ActivationHolder>) in.readObject();
        activationHolder = bcast.getValue();
    }

    public ActivationHolder getActivationHolder() {
        return activationHolder;
    }
}
