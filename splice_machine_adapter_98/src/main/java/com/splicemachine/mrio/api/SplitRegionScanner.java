package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
/*
 * 
 * Split Scanner for multiple region scanners
 * 
 */
public class SplitRegionScanner implements RegionScanner {
	protected List<RegionScanner> regionScanners = new ArrayList<RegionScanner>(2);	
	protected RegionScanner currentScanner;
	
	public void registerRegionScanner(RegionScanner regionScanner) {
		if (currentScanner != null)
			regionScanners.add(regionScanner);
		else
			currentScanner = regionScanner;
	}
	
	@Override
	public boolean next(List<Cell> results) throws IOException {
		boolean next = currentScanner.nextRaw(results);
		if (!next && !regionScanners.isEmpty()) {
			currentScanner = regionScanners.remove(0);
			return next(results);
		}
		return next;
	}

	@Override
	public boolean next(List<Cell> result, int limit) throws IOException {
		return next(result);
	}

	@Override
	public void close() throws IOException {
		currentScanner.close();
		for (RegionScanner rs: regionScanners) {
			rs.close();
		}
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return currentScanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() throws IOException {
		return currentScanner.isFilterDone();
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		throw new RuntimeException("Reseek not supported");
	}

	@Override
	public long getMaxResultSize() {
		return currentScanner.getMaxResultSize();
	}

	@Override
	public long getMvccReadPoint() {
		return currentScanner.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		return next(result);
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		return next(result, limit);
	}

}
