package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ResultScanner;

public class MemstoreKeyValueScanner extends BaseMemstoreKeyValueScanner<KeyValue> {
	
	public MemstoreKeyValueScanner(ResultScanner resultScanner) throws IOException {
		super(resultScanner);
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		return nextInternal(results);
	}

	@Override
	public boolean next(List<KeyValue> results, String metric)
			throws IOException {
		return nextInternal(results);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		return nextInternal(result);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric)
			throws IOException {
		return nextInternal(result);
	}


}