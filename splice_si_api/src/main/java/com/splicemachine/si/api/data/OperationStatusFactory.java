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

    /*
    .getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS;
     */


    /**
     * Return true if all is good, throw status specific exception if not.
     *
     * @return {@code true} if there is no error thrown
     * @throws IOException
     */
    boolean processPutStatus(MutationStatus operationStatus) throws IOException;

    /*    switch (operationStatuses[0].getOperationStatusCode()) {
            case NOT_RUN:
                throw new IOException("Could not acquire Lock");
            case BAD_FAMILY:
                throw dataLib.noSuchFamily(operationStatuses[0].getExceptionMsg());
            case SANITY_CHECK_FAILURE:
                throw new IOException("Sanity Check failure:" + operationStatuses[0].getExceptionMsg());
            case FAILURE:
                throw new IOException(operationStatuses[0].getExceptionMsg());
            default:
                return true;
         }
    */
    MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus);
    /*
    private OperationStatus getCorrectStatus(OperationStatus status, OperationStatus oldStatus) {
        switch (oldStatus.getOperationStatusCode()) {
            case SUCCESS:
                return status;
            case NOT_RUN:
            case BAD_FAMILY:
            case SANITY_CHECK_FAILURE:
            case FAILURE:
                return oldStatus;
        }
        return null;
    }
*/

    MutationStatus success();

    MutationStatus notRun();

    MutationStatus failure(String messsage);

    MutationStatus failure(Throwable t);

    ConstraintChecker getNoOpConstraintChecker();
}
