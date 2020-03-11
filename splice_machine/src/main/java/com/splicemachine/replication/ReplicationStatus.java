package com.splicemachine.replication;

import java.io.*;

public class ReplicationStatus implements Externalizable {

    private short peerId;
    private long replicationProgress;

    public ReplicationStatus() {

    }

    public ReplicationStatus(short peerId, long replicationProgress) {
        this.peerId = peerId;
        this.replicationProgress = replicationProgress;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(peerId);
        out.writeLong(replicationProgress);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        peerId = in.readShort();
        replicationProgress = in.readLong();
    }

    public byte[] toBytes() throws IOException{
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(this);
            out.flush();
            byte[] b = bos.toByteArray();
            return b;
        }
    }

    public static ReplicationStatus parseFrom(byte[] bs) throws  IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bs);
             ObjectInput in = new ObjectInputStream(bis)) {
            ReplicationStatus replicationStatus = (ReplicationStatus) in.readObject();
            return replicationStatus;
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public short getPeerId() {
        return peerId;
    }

    public long getReplicationProgress() {
        return replicationProgress;
    }

    public void setReplicationProgress(long replicationProgress) {
        this.replicationProgress = replicationProgress;
    }
}

