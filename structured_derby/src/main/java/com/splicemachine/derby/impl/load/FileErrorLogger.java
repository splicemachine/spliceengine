package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.writer.WriteResult;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Logs an erroneous row to a file.
 *
 * @author Scott Fines
 * Date: 3/7/14
 */
public class FileErrorLogger implements RowErrorLogger{
//		private static final Logger LOG = Logger.getLogger(FileErrorLogger.class);
		private final FileSystem fs;
		private final Path outputFile;
		private FSDataOutputStream outputStream;
		private BufferedWriter writer;

		private long rowsWritten = 0l;
		private long syncBufferInterval;

		public FileErrorLogger(FileSystem fs, Path outputFile, long syncBufferInterval) {
				this.fs = fs;
				this.outputFile = outputFile;
				long sbi = 1l;
				while(sbi<syncBufferInterval){
					sbi<<=1;
				}
				this.syncBufferInterval = sbi-1; //power of 2 for uber efficiency
		}

		@Override
		public void open() throws IOException {
				outputStream = fs.create(outputFile);
				//TODO -sf- do we want to compress the outputstream?
				writer = new BufferedWriter(new OutputStreamWriter(outputStream));
		}

		@Override
		public void close() throws IOException {
//				LOG.trace("Close called");
				//close the stream
				writer.flush();
				writer.close();
				outputStream.flush();
				outputStream.close();
//				LOG.trace("close completed");
		}

		@Override
		public void report(String row, WriteResult result) throws IOException {
				StringBuilder sb = new StringBuilder();
				switch(result.getCode()){
						case NOT_NULL:
								sb = sb.append("NOTNULL(").append(result.getErrorMessage()).append(")");
								break;
						case FAILED:
								sb =sb.append("ERROR(").append(result.getErrorMessage()).append(")");
								break;
						case WRITE_CONFLICT:
								sb = sb.append("WRITECONFLICT(").append(result.getErrorMessage()).append(")");
								break;
						case PRIMARY_KEY_VIOLATION:
								sb = sb.append("PRIMARYKEY");
								break;
						case UNIQUE_VIOLATION:
								sb = sb.append("UNIQUE");
								break;
						case FOREIGN_KEY_VIOLATION:
								sb = sb.append("FOREIGNKEY");
								break;
						case CHECK_VIOLATION:
								sb = sb.append("CHECK");
								break;
						case NOT_SERVING_REGION:
						case WRONG_REGION:
						case REGION_TOO_BUSY:
								sb = sb.append("ENVIRONMENT(").append(result.getCode()).append(")");
								break;
				}
				sb = sb.append("---").append(row);

//				LOG.trace("Writing line to writer "+ writer );
				writer.write(sb.toString());
				rowsWritten++;
//				if((rowsWritten & syncBufferInterval)==0){
//						writer.flush();
//						outputStream.hsync();
//				}

				writer.newLine();
		}

		@Override
		public void deleteLog() throws IOException {
			fs.delete(outputFile,false);
		}
}
