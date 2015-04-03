package com.splicemachine.utils.io;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.backup.Backup;
import com.splicemachine.utils.SpliceLogUtils;

public class IOUtils {

	protected static final Logger LOG = Logger.getLogger(IOUtils.class);

	private static Configuration getConfiguration() {
		return SpliceConstants.config;
	}

	private static Path getPath(FileSystem fs, Object file) throws IOException {
		if (file instanceof HFileLink) {
			HFileLink link = (HFileLink) file;
			return link.getAvailablePath(fs);
		} else {
			return (Path) file;
		}
	}

	public static void copyFileWithThrottling(FileSystem srcFS, Object file,
			FileSystem outputFs, Path outputPath, boolean deleteSource,
			Configuration conf) throws IOException {
		// Get the file information
		FileStatus inputStat = getSourceFileStatus(srcFS, file);
		// Verify if the output file exists and is the same that we want to copy
		if (outputFs.exists(outputPath)) {
			FileStatus outputStat = outputFs.getFileStatus(outputPath);
			if (outputStat != null && sameFile(inputStat, outputStat)) {
				SpliceLogUtils.info(LOG, "Skip copy " + inputStat.getPath()
						+ " to " + outputPath + ", same file.");
				return;
			}
		}
		InputStream in = openSourceFile(srcFS, file);
		int bandwidthMB = getConfiguration().getInt(Backup.CONF_BANDWIDTH_MB,
				100);
		if (Integer.MAX_VALUE != bandwidthMB) {
			in = new ThrottledInputStream(new BufferedInputStream(in),
					bandwidthMB * 1024 * 1024);
		}
		try {
			// Ensure that the output folder is there and copy the file
			outputFs.mkdirs(outputPath.getParent());
			FSDataOutputStream out = outputFs.create(outputPath, true);
			try {
				copyData(getPath(srcFS, file), in, outputPath, out,
						inputStat.getLen());
			} finally {
				out.close();
			}
		} finally {
			in.close();
		}
	}

	/**
	 * Check if the two files are equal by looking at the file length. (they
	 * already have the same name).
	 * 
	 */
	private static boolean sameFile(final FileStatus inputStat,
			final FileStatus outputStat) {
		// Not matching length
		if (inputStat.getLen() != outputStat.getLen())
			return false;
		return true;
	}

	private static FileStatus getSourceFileStatus(FileSystem fs, Object file)
			throws IOException {
		if (file instanceof HFileLink) {
			HFileLink link = (HFileLink) file;
			return link.getFileStatus(fs);
		} else {
			return fs.getFileStatus((Path) file);
		}
	}

	private static FSDataInputStream openSourceFile(FileSystem fs, Object file)
			throws IOException {
		if (file instanceof HFileLink) {
			return ((HFileLink) file).open(fs, Backup.IO_BUFFER_SIZE);
		} else {
			Path path = (Path) file;
			return fs.open(path, Backup.IO_BUFFER_SIZE);
		}
	}

	@SuppressWarnings("deprecation")
	private static void copyData(final Path inputPath, final InputStream in,
			final Path outputPath, final FSDataOutputStream out,
			final long inputFileSize) throws IOException {
		final String statusMessage = "copied %s/"
				+ StringUtils.humanReadableInt(inputFileSize) + " (%.1f%%)";

		try {
			byte[] buffer = new byte[Backup.IO_BUFFER_SIZE];
			long totalBytesWritten = 0;
			int reportBytes = 0;
			int bytesRead;

			long stime = System.currentTimeMillis();
			while ((bytesRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, bytesRead);
				totalBytesWritten += bytesRead;
				reportBytes += bytesRead;
				if (reportBytes >= Backup.IO_REPORT_SIZE) {

					if (LOG.isTraceEnabled())
						SpliceLogUtils
								.trace(LOG,
										String.format(
												statusMessage,
												StringUtils
														.humanReadableInt(totalBytesWritten),
												(totalBytesWritten / (float) inputFileSize) * 100.0f)
												+ " from "
												+ inputPath
												+ " to "
												+ outputPath);
					reportBytes = 0;
				}
			}
			long etime = System.currentTimeMillis();

			SpliceLogUtils
					.info(LOG,
							String.format(
									statusMessage,
									StringUtils
											.humanReadableInt(totalBytesWritten),
									(totalBytesWritten / (float) inputFileSize) * 100.0f)
									+ " from "
									+ inputPath
									+ " to "
									+ outputPath);

			// Verify that the written size match
			if (totalBytesWritten != inputFileSize) {
				String msg = "number of bytes copied not matching copied="
						+ totalBytesWritten + " expected=" + inputFileSize
						+ " for file=" + inputPath;
				throw new IOException(msg);
			}

			SpliceLogUtils.info(LOG, "copy completed for input=" + inputPath
					+ " output=" + outputPath);
			SpliceLogUtils
					.info(LOG,
							"size="
									+ totalBytesWritten
									+ " ("
									+ StringUtils
											.humanReadableInt(totalBytesWritten)
									+ ")"
									+ " time="
									+ StringUtils.formatTimeDiff(etime, stime)
									+ String.format(
											" %.3fM/sec",
											(totalBytesWritten / ((etime - stime) / 1000.0)) / 1048576.0));
		} catch (IOException e) {
			LOG.error("Error copying " + inputPath + " to " + outputPath, e);
			throw e;
		}
	}

}
