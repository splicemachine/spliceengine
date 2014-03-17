package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * mapping between a KeyValueScanner and an InternalScanner
 *
 * @author Scott Fines
 * Date: 11/20/13
 */
public class KeyValueInternalScanner implements InternalScanner {
		private final KeyValueScanner delegate;

		public KeyValueInternalScanner(KeyValueScanner delegate) {
				this.delegate = delegate;
		}

		@Override
		public boolean next(List<Cell> results) throws IOException {
				return next(results);
		}

		@Override
		public boolean next(List<Cell> result, int limit) throws IOException {
				return next(result,limit);
		}

		@Override
		public void close() throws IOException {
				delegate.close();
		}
}
