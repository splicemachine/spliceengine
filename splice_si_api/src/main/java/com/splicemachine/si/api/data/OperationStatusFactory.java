package com.splicemachine.si.api.data;

import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * Factory for creating the proper MutationStatus for the given architecture.
 *
 * Created by jleach on 12/9/15.
 */
public interface OperationStatusFactory{

    /**
     * Return true if all is good, throw status specific exception if not.
     *
     * @return {@code true} if there is no error thrown
     * @throws IOException
     */
    boolean processPutStatus(MutationStatus operationStatus) throws IOException;

    MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus);

    MutationStatus success();

    MutationStatus notRun();

    MutationStatus failure(String message);

    MutationStatus failure(Throwable t);

    ConstraintChecker getNoOpConstraintChecker();
}
