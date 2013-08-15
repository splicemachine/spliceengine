package com.splicemachine.derby.utils;

import javax.management.MXBean;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 8/15/13
 */
@MXBean
public interface ErrorReport {

    public List<String> getRecentThrowingClassNames();

    public List<String> getRecentReportingClassNames();

    public Map<String,Long> getMostRecentErrors();

    public long getTotalErrors();

    public long getTotalIOExceptions();

    public long getTotalDoNotRetryIOExceptions();

    public long getTotalStandardExceptions();

    public long getTotalExecutionExceptions();

    public long getTotalRuntimeExceptions();
}
