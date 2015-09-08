package com.splicemachine.mrio.api.core;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.List;

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

    public boolean seek(KeyValue key) throws IOException{
		return seek((Cell)key);
    }

//	@Override
    public boolean reseek(KeyValue key) throws IOException{
		return reseek((Cell)key);
    }

//	@Override
    public boolean requestSeek(KeyValue kv,boolean forward,boolean useBloom) throws IOException{
		return requestSeek((Cell)kv,forward,useBloom);
    }

//	@Override
    public boolean backwardSeek(KeyValue key) throws IOException{
		return backwardSeek((Cell)key);
	}

//	@Override
    public boolean seekToPreviousRow(KeyValue key) throws IOException{
		return seekToPreviousRow((Cell)key);
    }

	@Override
	public boolean backwardSeek(Cell key) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "backwardSeek");		
		return false;
	}
	@Override
	public boolean seekToPreviousRow(Cell key) throws IOException {
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

//    @Override
    public byte[] getNextIndexedKey() {
        // TODO: JC - fudged this to get compile against 0.98.12-mapr with HBASE-13420.patch
        return null;
    }
}