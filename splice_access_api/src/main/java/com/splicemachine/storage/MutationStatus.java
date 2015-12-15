package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface MutationStatus{

    boolean isSuccess();

    boolean isFailed();

    boolean isNotRun();

    String errorMessage();

    MutationStatus getClone();
}
