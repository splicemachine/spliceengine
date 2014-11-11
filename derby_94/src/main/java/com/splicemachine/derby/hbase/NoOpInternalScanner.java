package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

public class NoOpInternalScanner implements InternalScanner{

		@Override public boolean next(List<KeyValue> results) throws IOException { return false;   }

		@Override public boolean next(List<KeyValue> results, String metric) throws IOException { return false;}

		@Override public boolean next(List<KeyValue> result, int limit) throws IOException { return false;}

		@Override public boolean next(List<KeyValue> result, int limit, String metric) throws IOException { return false; }

		@Override public void close() throws IOException { }
}
