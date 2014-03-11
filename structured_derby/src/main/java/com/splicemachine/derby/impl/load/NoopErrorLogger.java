package com.splicemachine.derby.impl.load;

import com.splicemachine.hbase.writer.WriteResult;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/10/14
 */
public class NoopErrorLogger implements RowErrorLogger{
		public static final RowErrorLogger INSTANCE = new NoopErrorLogger();

		@Override public void report(String row, WriteResult result) throws IOException {  }

		@Override public void deleteLog() throws IOException {  }

		@Override public void open() throws IOException {  }

		@Override public void close() throws IOException {  }
}
