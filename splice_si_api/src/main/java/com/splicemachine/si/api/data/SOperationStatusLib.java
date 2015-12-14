package com.splicemachine.si.api.data;

import java.io.IOException;

/**
 * Created by jleach on 12/9/15.
 */
public interface SOperationStatusLib<OperationStatus> {

    public OperationStatus getNotRun(); // NOT_RUN;
    public OperationStatus getSuccess();
    public OperationStatus getFailure();
    public boolean isSuccess(OperationStatus operationStatus);
    /*
    operationStatus.getOperationStatusCode() != HConstants.OperationStatusCode.SUCCESS
     */
    public OperationStatus[] getArrayOfSize(int size);
    /*
    .getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS;
     */


    /**
     * Return true if all is good, throw status specific exception if not.
     *
     * @return
     * @throws IOException
     */
    public boolean processPutStatus(OperationStatus operationStatus) throws IOException;
/*    switch (operationStatuses[0].getOperationStatusCode()) {
        case NOT_RUN:
            throw new IOException("Could not acquire Lock");
        case BAD_FAMILY:
            throw dataLib.getNoSuchColumnFamilyException(operationStatuses[0].getExceptionMsg());
        case SANITY_CHECK_FAILURE:
            throw new IOException("Sanity Check failure:" + operationStatuses[0].getExceptionMsg());
        case FAILURE:
            throw new IOException(operationStatuses[0].getExceptionMsg());
        default:
            return true;
     }
*/
    public OperationStatus getCorrectStatus(OperationStatus status, OperationStatus oldStatus);
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

    public OperationStatus getAdditiveWriteConflictOperationStatus();

    /*

    new OperationStatus(HConstants.OperationStatusCode.FAILURE, dataLib.getAdditiveWriteConflict().getMessage());
     */

    public OperationStatus newFailureOperationStatus(Exception e);

    /*

    new OperationStatus(HConstants.OperationStatusCode.FAILURE, ex.getMessage());
     */
}
