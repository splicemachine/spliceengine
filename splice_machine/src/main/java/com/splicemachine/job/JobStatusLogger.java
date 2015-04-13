package com.splicemachine.job;

import java.io.IOException;

/**
 * Interface for logging the status of a running job.
 *
 * @author dwinters
 */
public interface JobStatusLogger {

	/**
	 * Open the import log file.
	 * @throws IOException
	 */
	public abstract void openLogFile() throws IOException;

	/**
	 * Close the import log file.
	 * @throws IOException
	 */
	public abstract void closeLogFile();

	/**
	 * Write a message to the import log file.
	 * @param msg the message to log
	 * @throws IOException
	 */
	public abstract void log(String msg);

	/**
	 * Write a message to the import log file and do not append a newline on the end.
	 * @param msg the message to log
	 * @throws IOException
	 */
	public abstract void logString(String msg);
}
