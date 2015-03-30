package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.splicemachine.mrio.api.core.BaseMemstoreKeyValueScanner;
import com.splicemachine.utils.SpliceLogUtils;

public class MemstoreKeyValueScanner extends BaseMemstoreKeyValueScanner<Cell> {
	
	public MemstoreKeyValueScanner(ResultScanner resultScanner) throws IOException {
		super(resultScanner);
	}

	public boolean next(List<Cell> results) throws IOException {
		return nextInternal(results);
	}

	public boolean next(List<Cell> results, int limit) throws IOException {
		return this.nextInternal(results);
	}
	@Override
	public boolean backwardSeek(KeyValue key) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "backwardSeek");		
		return false;
	}
	@Override
	public boolean seekToPreviousRow(KeyValue key) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "seekToPreviousRow %s", key);		
		return false;
	}
	@Override
	public boolean seekToLastRow() throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "seekToLastRow");		
		return false;
	}
}