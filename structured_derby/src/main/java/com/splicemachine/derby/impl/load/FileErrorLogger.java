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
		private final FileSystem fs;
		private final Path outputFile;
		private FSDataOutputStream outputStream;
		private BufferedWriter writer;

		public FileErrorLogger(FileSystem fs, Path outputFile) {
				this.fs = fs;
				this.outputFile = outputFile;
		}

		@Override
		public void close() throws IOException {
				if(writer==null) return; //nothing to do

				//close the stream
				writer.flush();
				writer.close();
				outputStream.close();
		}

		@Override
		public void report(String row, WriteResult result) throws IOException {
				if(outputStream==null){
						outputStream = fs.create(outputFile);
						//TODO -sf- do we want to compress the outputstream?
						writer = new BufferedWriter(new OutputStreamWriter(outputStream));
				}

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

				writer.write(sb.toString());
				writer.newLine();
		}
}
