package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

public class NoOpInternalScanner implements InternalScanner{

	@Override
	public boolean next(List<Cell> results) throws IOException {
		return false;
	}

	@Override
	public boolean next(List<Cell> result, int limit) throws IOException {
		return false;
	}

	@Override
	public void close() throws IOException {		
	}

		
}
