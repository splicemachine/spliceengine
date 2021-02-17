package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

// somehow we can't use DirectPipelineExceptionFactory (No PipelineTestDataEnv found)
public class PipelineExceptionFactoryForTest implements PipelineExceptionFactory {
    @Override
    public IOException primaryKeyViolation(ConstraintContext constraintContext) {
        return null;
    }

    @Override
    public IOException foreignKeyViolation(ConstraintContext constraintContext) {
        return null;
    }

    @Override
    public IOException uniqueViolation(ConstraintContext constraintContext) {
        return null;
    }

    @Override
    public IOException notNullViolation(ConstraintContext constraintContext) {
        return null;
    }

    @Override
    public Throwable processPipelineException(Throwable t) {
        return null;
    }

    @Override
    public boolean needsTransactionalRetry(Throwable t) {
        return false;
    }

    @Override
    public boolean canFinitelyRetry(Throwable t) {
        return false;
    }

    @Override
    public boolean canInfinitelyRetry(Throwable t) {
        return false;
    }

    @Override
    public Exception processErrorResult(WriteResult value) {
        return null;
    }

    @Override
    public IOException fromErrorString(String s) {
        return null;
    }

    @Override
    public boolean isHBase() {
        return false;
    }

    @Override
    public IOException writeWriteConflict(long txn1, long txn2) {
        return null;
    }

    @Override
    public IOException readOnlyModification(String message) {
        return null;
    }

    @Override
    public IOException noSuchFamily(String message) {
        return null;
    }

    @Override
    public IOException transactionTimeout(long tnxId) {
        return null;
    }

    @Override
    public IOException cannotCommit(long txnId, Txn.State actualState) {
        return null;
    }

    @Override
    public IOException cannotCommit(String message) {
        return null;
    }

    @Override
    public IOException additiveWriteConflict() {
        return null;
    }

    @Override
    public IOException doNotRetry(String message) {
        return null;
    }

    @Override
    public IOException doNotRetry(Throwable t) {
        return null;
    }

    @Override
    public IOException processRemoteException(Throwable e) {
        return null;
    }

    @Override
    public IOException callerDisconnected(String message) {
        return null;
    }

    @Override
    public IOException failedServer(String message) {
        return null;
    }

    @Override
    public IOException notServingPartition(String s) {
        return null;
    }

    @Override
    public IOException connectionClosingException() {
        return null;
    }

    @Override
    public boolean allowsRetry(Throwable error) {
        return false;
    }
}
