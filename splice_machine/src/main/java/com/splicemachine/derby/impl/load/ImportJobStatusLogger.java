package com.splicemachine.derby.impl.load;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.job.JobStatusLogger;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class for logging the status of a running import job.  The status is written to a log file in the 'bad' directory of the file(s) to import.
 *
 * @author dwinters
 */
public class ImportJobStatusLogger implements JobStatusLogger {
	private static Logger LOG = Logger.getLogger(ImportJobStatusLogger.class);
	private FSDataOutputStream logFileOut;
	private ImportContext importContext;
	private String fileId;

	/**
	 * Used by the log method which is synchronized.
	 */
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static final String IMPORT_LOG_PREFIX= "importStatus";

	public ImportJobStatusLogger(ImportContext importContext,String fileId) throws IOException {
		this.importContext = importContext;
		this.fileId=fileId;
		openLogFile();
	}

	@Override
	public void openLogFile() throws IOException {
		// First, try to put the log file in the 'bad' directory where the import tasks write the failed record files.
		Path directory = importContext.getBadLogDirectory();
		// Next, if the bad directory was not specified, put the log file in the parent of the import directory/file.
		if (directory == null) {
			directory = importContext.getFilePath().getParent();
		}
		FileSystem fs = directory.getFileSystem(SpliceConstants.config);

		String fileName=IMPORT_LOG_PREFIX+"-"+fileId+".log";
		Path logFilePath = new Path(directory,fileName);
		this.logFileOut = fs.create(logFilePath, true);
	}

	@Override
	public void closeLogFile() {
		try {
			this.logFileOut.close();
		} catch (IOException e) {
			LOG.error("Failed to close the import log file", e);
		}
	}

	@Override
	public void log(String msg) {
		logString(msg + "\n");
	}

	@Override
	public synchronized void logString(String msg) {
		if (LOG.isInfoEnabled()) {
			LOG.info(msg);
		}
		try {
			this.logFileOut.writeBytes(String.format("%s %s", dateFormat.format(new Date()), msg));
			this.logFileOut.flush();
            this.logFileOut.hsync();
		} catch (IOException e){
			LOG.error("Failed to write to the import log file", e);
		}
	}
}
