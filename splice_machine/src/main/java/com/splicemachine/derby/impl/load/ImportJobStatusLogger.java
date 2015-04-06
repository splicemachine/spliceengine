package com.splicemachine.derby.impl.load;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.job.JobStatusLogger;

/**
 * Class for logging the status of a running import job.  The status is written to a log file in the 'bad' directory of the file(s) to import.
 *
 * @author dwinters
 */
public class ImportJobStatusLogger implements JobStatusLogger {
	private static Logger LOG = Logger.getLogger(ImportJobStatusLogger.class);
	private FSDataOutputStream logFileOut;
	private ImportContext importContext;

	public static final String IMPORT_LOG_FILE_NAME = "importStatus.log";

	public ImportJobStatusLogger(ImportContext importContext) throws IOException {
		this.importContext = importContext;
		openLogFile();
	}

	/* (non-Javadoc)
	 * @see com.splicemachine.derby.impl.load.JobLogger#openLogFile()
	 */
	@Override
	public void openLogFile() throws IOException {
		// Keep the log file in the 'bad' directory since we are sure to have write access to it.
		FileSystem fs = importContext.getBadLogDirectory().getFileSystem(SpliceConstants.config);

		Path logFilePath = new Path(importContext.getBadLogDirectory(), IMPORT_LOG_FILE_NAME);
		this.logFileOut = fs.create(logFilePath, true);
	}

	/* (non-Javadoc)
	 * @see com.splicemachine.derby.impl.load.JobLogger#closeLogFile()
	 */
	@Override
	public void closeLogFile() {
		try {
			this.logFileOut.close();
		} catch (IOException e) {
			LOG.error("Failed to close the import log file", e);
		}
	}

	/* (non-Javadoc)
	 * @see com.splicemachine.derby.impl.load.JobLogger#log(java.lang.String)
	 * Make this method synchronized since both the job and task status threads may attempt to log status at the same time.
	 */
	@Override
	public synchronized void log(String msg) {
		if (LOG.isInfoEnabled()) {
			LOG.info(msg);
		}
		try {
			this.logFileOut.writeBytes(msg + "\n");
			this.logFileOut.flush();
		} catch (IOException e){
			LOG.error("Failed to write to the import log file", e);
		}
	}
}
