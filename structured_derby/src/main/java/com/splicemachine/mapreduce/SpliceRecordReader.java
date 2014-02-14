package com.splicemachine.mapreduce;

import java.io.IOException;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.mapred.RecordReader;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;

public class SpliceRecordReader implements RecordReader<HBaseRowLocation,ExecRow> {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public HBaseRowLocation createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExecRow createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(HBaseRowLocation arg0, ExecRow arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}


}
