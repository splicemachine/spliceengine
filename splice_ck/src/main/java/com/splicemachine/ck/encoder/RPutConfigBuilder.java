package com.splicemachine.ck.encoder;

public class RPutConfigBuilder {
    RPutConfig result = new RPutConfig();

    public RPutConfigBuilder withTombstone() {
        result.setTombstone();
        return this;
    }

    public RPutConfigBuilder withAntiTombstone() {
        result.setAntiTombstone();
        return this;
    }

    public RPutConfigBuilder withFirstWrite() {
        result.setFirstWrite();
        return this;
    }

    public RPutConfigBuilder withDeleteAfterFirstWrite() {
        result.setDeleteAfterFirstWrite();
        return this;
    }

    public RPutConfigBuilder withForeignKeyCounter(long counter) {
        result.setForeignKeyCounter(counter);
        return this;
    }

    public RPutConfigBuilder withUserData(String userData) {
        result.setUserData(userData);
        return this;
    }

    public RPutConfigBuilder withCommitTS(long commitTS) {
        result.setCommitTS(commitTS);
        return this;
    }

    public RPutConfigBuilder withTxnId(long txnId) {
        result.setTxnId(txnId);
        return this;
    }

    public RPutConfig getConfig() {
        return result;
    }
}
