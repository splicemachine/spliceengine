package com.splicemachine.si.api.data;

/**
 *
 * Library for Understanding Filter Return Codes
 *
 */
public interface SReturnCodeLib<ReturnCode> {
    boolean returnCodeIsInclude(ReturnCode returnCode);
    ReturnCode getIncludeReturnCode();
    ReturnCode getNextRowReturnCode();
    ReturnCode getSkipReturnCode();
    ReturnCode getNextColReturnCode();
    ReturnCode getSeekNextUsingHint();
    boolean isNextCol(ReturnCode returnCode);
    boolean isNextRow(ReturnCode returnCode);
    boolean isSeekNextUsingHint(ReturnCode returnCode);
    boolean isSkip(ReturnCode returnCode);
    boolean isInclude(ReturnCode returnCode);
    boolean isIncludeAndNextCol(ReturnCode returnCode);
}
