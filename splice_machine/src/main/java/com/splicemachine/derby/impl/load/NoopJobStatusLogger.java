package com.splicemachine.derby.impl.load;

import com.splicemachine.job.JobStatusLogger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 10/12/15
 */
public class NoopJobStatusLogger implements JobStatusLogger{
    public static JobStatusLogger INSTANCE = new NoopJobStatusLogger();

    @Override public void openLogFile() throws IOException{ }
    @Override public void closeLogFile(){ }
    @Override public void log(String msg){ }
    @Override public void logString(String msg){ }
}
