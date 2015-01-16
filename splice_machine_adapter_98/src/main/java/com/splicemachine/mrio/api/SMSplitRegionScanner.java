package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.ClientSideRegionScanner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

public class SMSplitRegionScanner implements KeyValueScanner {

	@Override
	public KeyValue peek() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue next() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean seek(KeyValue key) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean reseek(KeyValue key) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getSequenceID() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
			long oldestUnexpiredTS) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
			throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean realSeekDone() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void enforceSeek() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isFileScanner() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean backwardSeek(KeyValue key) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean seekToPreviousRow(KeyValue key) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean seekToLastRow() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}


}
