package com.splicemachine.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import java.io.IOException;
import java.util.List;

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
				KeyValue next = delegate.next();
				if(next!=null)
						results.add(next);
				return next!=null;
		}

		@Override
		public boolean next(List<Cell> result, int limit) throws IOException {
				for(int i=0;i<limit;i++){
						if(!next(result))return false;
				}
				return true;
		}

		@Override
		public void close() throws IOException {
				delegate.close();
		}
}