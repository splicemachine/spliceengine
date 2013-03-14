package com.splicemachine.constants;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface ITransactionManager {
    ITransactionState beginTransaction() throws KeeperException, InterruptedException, IOException, ExecutionException;

    int prepareCommit(final ITransactionState transactionState) throws KeeperException, InterruptedException, IOException;
    void doCommit(final ITransactionState transactionState) throws KeeperException, InterruptedException, IOException;
    void abort(final ITransactionState transactionState) throws IOException, KeeperException, InterruptedException;
    void prepareCommit2(Object bonus, ITransactionState transactionState) throws KeeperException, InterruptedException, IOException;
}
