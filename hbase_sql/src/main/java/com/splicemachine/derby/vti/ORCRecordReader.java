package com.splicemachine.derby.vti;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.log4j.Logger;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

/**
 * This is iterator for the records in the ORC files. It supports either a
 * single file or Files in a folder. In case of file the next() will return the
 * rows from one file, and then move to the next file
 * 
 * @author jramineni
 * 
 */

public class ORCRecordReader implements Iterable<LocatedRow> {

	private static final Logger LOG = Logger.getLogger(ORCRecordReader.class);

	// File System
	private FileSystem filesystem;

	// File or Directory Path
	private Path filePath;

	// The format of the Record to be returned
	private ExecRow execRow;

	public ORCRecordReader(FileSystem filesystem, Path filePath, ExecRow execRow) {
		this.filesystem = filesystem;
		this.filePath = filePath;
		this.execRow = execRow;

	}

	@Override
	public Iterator<LocatedRow> iterator() {
		return new ORCRecordReaderIterator();
	}

	private class ORCRecordReaderIterator implements Iterator<LocatedRow>,
	Closeable {

		// This is contains the details of the ORC File Record format
		private StructObjectInspector inspector;

		// Records in Orc File to be processed
		private RecordReader records;

		// Current row in the ORC File to process
		private Object row = null;

		// Indicates if a single file or files in folder are considered
		private boolean isDir = false;

		// Iterator for files in a folder, when folder is specified
		private RemoteIterator<LocatedFileStatus> fileList = null;

		public ORCRecordReaderIterator() {
			try {
				Path curFiletoProcess = null;
				Reader reader;

				// Check if filePath specifes afile or folder
				// If its folder, set the flag, and get the first file in the
				// folder
				if (filesystem.isDirectory(filePath)) {
					isDir = true;
					this.fileList = filesystem.listFiles(filePath, false);
					curFiletoProcess = fileList.next().getPath();
				} else {

					curFiletoProcess = filePath;
				}

				// Get the reader for the single file (first file in case of
				// folder)
				reader = getReader(curFiletoProcess);

				// Get the inspector for the format of the record in the ORC
				// File
				this.inspector = (StructObjectInspector) reader
						.getObjectInspector();

				// Retrieve the Records from reader to process
				records = reader.rows();

			} catch (Exception e) {
				try {
					if (records != null)
						records.close();
				} catch (Exception cE) {
					throw new RuntimeException(cE);
				}
				throw new RuntimeException(e);
			}

		}

		/*
		 * This checks if there are more records to process. First checks if
		 * there are more records in current file. If no more records, gets the
		 * next file and checks that for more records.
		 */
		@Override
		public boolean hasNext() {
			try {

				// If no more records in current file
				if (!records.hasNext()) {
					// check if there is next file
					if (this.hasNextFile()) {
						// Next file exists, so get the records from that file
						getNextFileRecords();
					}
				}

				return records.hasNext();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		/*
		 * This formats the record in the ORC file and return as LocatedRow
		 */
		@Override
		public LocatedRow next() {
			try {
				// Return Row Format
				// Changed for performance
				//Reverting it back, since on import i.e. using insert with splice properties, inserted rows
				// with all rows identical, the values of the last row.
				 ExecRow returnRow = execRow.getClone();
				//ExecRow returnRow = execRow;
				// Get the row
				
				row = records.next(row);

				// Get the row details
				List<Object> value_lst = inspector
						.getStructFieldsDataAsList(row);

				String value = null;
				// Construct row for each column
				for (int i = 1; i <= execRow.nColumns(); i++) {
					Object field = value_lst.get(i - 1);
					value = null;
					if (field != null)
						value = field.toString();
					// Note: Date is in same format as Splice Default,
					// For timestamp format yyyy-mm-dd hh:mm:ss[.f...]., Check
					// if
					// Timestamp.valueOf(value) is required
					returnRow.getColumn(i).setValue(value);
				}
				return new LocatedRow(returnRow);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		/*
		 * This checks if there are more files to process in the folder
		 */
		public boolean hasNextFile() {
			try {
				// If it is folder, check the file list iterator for more files
				if (isDir)
					return fileList.hasNext();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			// If it is single file, return false
			return false;
		}

		/*
		 * Get the the next ORC fiel to process in the folder
		 */
		public Path nextFile() {
			try {
				// IF it is folder, get the next file from the file list
				// iterator
				if (isDir)
					return fileList.next().getPath();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return null;
		}

		/*
		 * Returns Records in the next file
		 */
		public void getNextFileRecords() throws IOException {
			// Close the current records iterator to release the resources for
			// current file
			records.close();

			// Get the Record Reader for the next file
			Reader reader = getReader(this.nextFile());
			// set the records iterator for the new file
			this.records = reader.rows();
		}

		/*
		 * Gets the RecordReader of the specified file
		 */
		private Reader getReader(Path toProcessPath) throws IOException {
			Reader reader = null;
			if (toProcessPath == null) {
				throw new IOException("Null File Path");
			}
			reader = OrcFile.createReader(filesystem, toProcessPath);

			return reader;

		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
			if (records != null) {

				records.close();

			}
		}

	}
}
