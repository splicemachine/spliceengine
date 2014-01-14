package com.splicemachine.hbase;

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
		public boolean next(List<KeyValue> results) throws IOException {
				return next(results,null);
		}

		@Override
		public boolean next(List<KeyValue> results, String metric) throws IOException {
				KeyValue next = delegate.next();
				if(next!=null)
						results.add(next);
				return next!=null;
		}

		@Override
		public boolean next(List<KeyValue> result, int limit) throws IOException {
				return next(result,limit,null);
		}

		@Override
		public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
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
