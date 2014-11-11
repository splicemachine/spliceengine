package com.splicemachine.impl;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

public class IteratorRegionScanner implements RegionScanner{
	private final PeekingIterator<Set<KeyValue>> kvs;
	private final Scan scan;
	private final Filter filter;

	public IteratorRegionScanner(Iterator<Set<KeyValue>> kvs, Scan scan) {
			this.kvs = Iterators.peekingIterator(kvs);
			this.scan = scan;
			this.filter = scan.getFilter();
	}

	@Override public HRegionInfo getRegionInfo() { return mock(HRegionInfo.class); }

	@Override
	public boolean isFilterDone() {
			return filter!=null && filter.filterAllRemaining();
	}

	@SuppressWarnings("LoopStatementThatDoesntLoop")
	@Override
	public boolean reseek(byte[] row) throws IOException {
			if(!kvs.hasNext()) return false;
			while(kvs.hasNext()){
					Set<KeyValue> next = kvs.peek();
					if(next.size()<0){
							kvs.next();  //throw empty rows away
							continue;
					}
					for(KeyValue kv: next){
							if(Bytes.equals(kv.getBuffer(),kv.getRowOffset(),kv.getRowLength(),row,0, row.length))
									return true;
							else{
									kvs.next();
									break;
							}
					}
			}
			return false;
	}

	@Override public long getMvccReadPoint() { return 0; }

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
			return next(result);
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
			return next(result);
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
			OUTER: while(kvs.hasNext()){
					Set<KeyValue> next = kvs.next();
					List<KeyValue> toAdd = Lists.newArrayListWithCapacity(next.size());
					for(KeyValue kv:next){
							if(!containedInScan(kv)) continue; //skip KVs we aren't interested in
							if(filter!=null){
									Filter.ReturnCode retCode = filter.filterKeyValue(kv);
									switch(retCode){
											case INCLUDE:
											case INCLUDE_AND_NEXT_COL:
													toAdd.add(kv);
													break;
											case SKIP:
											case NEXT_COL:
													break;
											case NEXT_ROW:
													continue OUTER;
											case SEEK_NEXT_USING_HINT:
													throw new UnsupportedOperationException("DON'T USE THIS");
									}
							}
					}
					if(filter!=null){
        if(filter.filterRow()) continue;

							filter.filterRow(toAdd);
    }
					if(toAdd.size()>0){
							results.addAll(toAdd);
							return true;
					}
			}
			return false;
	}

	private boolean containedInScan(KeyValue kv) {
			byte[] family = kv.getFamily();
			Map<byte[], NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
if(familyMap.size()<=0) return true;

			if(!familyMap.containsKey(family)) return false;
			NavigableSet<byte[]> qualifiersToFetch = familyMap.get(family);
if(qualifiersToFetch.size()<=0) return true;
			return qualifiersToFetch.contains(kv.getQualifier());
	}

	@Override
	public boolean next(List<KeyValue> results, String metric) throws IOException {
			return next(results);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
			return next(result);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
			return next(result);
	}

	@Override
	public void close() throws IOException {
			//no-op
	}
}