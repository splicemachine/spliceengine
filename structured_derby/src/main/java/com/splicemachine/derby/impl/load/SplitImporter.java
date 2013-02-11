package com.splicemachine.derby.impl.load;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Imports data in as parallel a manner as possible from a file in HDFS.
 *
 * In this class,we try to be smart with our import code (Stupid idea). The reasoning is something like this:
 *
 * We want to preserve data locality as much as humanly possible; this data locality is in two parts:
 * HBase Region Locality, and HDFS file block locality. E.g. If a RegionServer has both a Region for
 * this table *and* a block from the HDFS file, that's our best case scenario.
 *
 * If a RegionServer contains a Region for this table, then we want it to process whatever it
 * reasonably can while preserving data locality. Thus, we look for any BlockLocations which have a replica
 * residing on that RegionServer, which will allow us to import that block with the utmost of Data locality.
 *
 * It is not reasonable to expect that there is a Region for this table available on every
 * RegionServer in the cluster. This means that we will not be able to use the cluster to its fullest,
 *  because Coprocessors do not allow us to submit tasks to RegionServers without a Region. This means that,
 *  if we just tied every block location to a Region right off the bat, we would end up with
 *
 *  A) Very bad data locality, since some BlockLocations will not be imported off of a local replica when they
 *  otherwise could have been.
 *
 *  B). Bad Parallelism, since only a subset of the full cluster (potentially just one single RegionServer!)
 *  will be used to perform the import process.
 *
 * So, we can't tie every BlockLocation to a Region and execute the import in a single parallel operation.
 * Instead, we must do this in an interative approach, where in each round we relocate all existing regions,
 * tie those to BlockLocations which exist on the same physical server, and import those in parallel.
 *
 * There will come a time, though, when one of the following states becomes true:
 *
 * 1. There are no more BlockLocations to import (yippee!). This is awesome because we managed
 * to tie every BlockLocation to a Region, and thus the file was imported with complete data locality.
 * 2. All RegionServers have a Region for this table. This means that we have a Region on every RegionServer,
 * so our Coprocessor execution is about as parallel as we can get, and as data-local as we can get.
 * 3. The import process is no longer causing Replica splits to occur, for some reason. This is unlikely
 * to occur unless #2 is also true, but it could happen if there are lots of duplicates in the file, but
 * we have some sort of deduplication happening on the insert code. This is mostly in here to prevent
 * runaway situations where the import loop never terminates because of some weird edge case causing
 * splits not to happen.
 *
 * In case #1, the file is finished importing, and so are we.
 *
 * In case #2 and #3, we have reached our maximum possible parallelism, so there's really no point in
 * continuing our iteration. So at this point ,we terminate our loop, and spread all our
 * remaining BlockLocations out amongst all the available regions, and do a final import stage. Then, we
 * can safely complete our tasks.
 */
public class SplitImporter implements Importer{
	private static final Logger LOG = Logger.getLogger(SplitImporter.class);
	private final ImportContext context;
	private HBaseAdmin admin;

	public SplitImporter(HBaseAdmin admin, ImportContext context){
		this.admin = admin;
		this.context = context;
	}

		@Override
		public long importData() throws IOException {
			//get all the blocks for the file to import
			FileSystem fs = FileSystem.get(SpliceUtils.config);
			if (!fs.exists(context.getFilePath()))
				throw new FileNotFoundException("Unable to find file "+context.getFilePath()+
						"in FileSystem. Did you put it into HDFS?");
			FileStatus status = fs.getFileStatus(context.getFilePath());
			BlockLocation[] locations = fs.getFileBlockLocations(status, 0, status.getLen());

			//get the total number of region servers we have to work with
			//ultimately, we'll get to a situation where there are no
			int allRegionSize = admin.getClusterStatus().getServers().size();

			byte[] tableBytes = Bytes.toBytes(Long.toString(context.getTableId()));
			List<HRegionLocation> regions = getRegionLocations(tableBytes);

			//get the conglom id for the table
			HTableInterface htable = SpliceAccessManager.getHTable(tableBytes);

			//keep a count of how many rows we've imported
			final AtomicLong rowsImported = new AtomicLong(0l);

			Multimap<HRegionLocation, BlockLocation> regionToBlockMap = ArrayListMultimap.create();
			Map<HRegionLocation, Integer> taskSizeMap = Maps.newHashMapWithExpectedSize(regions.size());
			int oldCount;
			boolean locationsLeft;
			do {
				//clear out our previous run's regionMap, and reset our regionSize counter
				oldCount = regions.size();
				regionToBlockMap.clear();

				//find all the regions which have a replica
				locationsLeft = false;
				for (int i = 0; i < locations.length; i++) {
					BlockLocation location = locations[i];
					if (location == null) continue;

					//we have a location that needs to be allocated
					taskSizeMap.clear();
					populateRegionLocationMatches(location, regions, taskSizeMap);

					if (taskSizeMap.size() > 0) {
						regionToBlockMap.put(getLeastLoadedRegion(taskSizeMap), location);
						locations[i] = null; //remove this from locations to do
					} else {
						//There are no regions (currently) which are tied to this BlockLocation
						//ignore this location--we'll deal with it later.
						locationsLeft = true;
					}
				}
				//we've populated all the blocks that we can in this round, time to submit them
				submitAndWait(htable, rowsImported, regionToBlockMap);

				//refresh the regions list in light of the new inserts
				regions = getRegionLocations(tableBytes);
			} while (locationsLeft && regions.size() != oldCount && regions.size() != allRegionSize);

			//we've run out of regions which map to Block Locations and/or we have put a region on every
			//server. No more waiting for regions, submit all remaining block locations for processing.
			if (locationsLeft) {
				regionToBlockMap.clear();
				for(BlockLocation location:locations){
					if (location == null) continue; //already dealt with this location, no need to do it again

					taskSizeMap.clear();
					populateRegionLocationMatches(location, regions, taskSizeMap);
					if (taskSizeMap.size() > 0) {
						regionToBlockMap.put(getLeastLoadedRegion(taskSizeMap), location);
					} else {
						//we don't match a region, so just submit it to the least loaded Region we have
						HRegionLocation smallLoc = null;
						int smallCount = Integer.MAX_VALUE;
						for (HRegionLocation region : regions) {
							if (!regionToBlockMap.containsKey(region)) {
								//found a Region without any blocks, shove it there
								smallLoc = region;
								break;
							} else if (smallCount < regionToBlockMap.get(region).size()) {
								smallLoc = region;
								smallCount = regionToBlockMap.get(region).size();
							}
						}
						regionToBlockMap.put(smallLoc, location);
					}
				}
				submitAndWait(htable, rowsImported, regionToBlockMap);
			}
			return rowsImported.get();
		}

	private List<HRegionLocation> getRegionLocations(byte[] tableBytes) throws IOException {
		/*
		 * Get the RegionLocations for a table based on the HRegionInfos returned.
		 */
		List<HRegionInfo> tableRegions = admin.getTableRegions(tableBytes);

		List<HRegionLocation> regions = Lists.newArrayListWithCapacity(tableRegions.size());
		for (HRegionInfo tableRegion : tableRegions) {
			regions.add(admin.getConnection().locateRegion(tableRegion.getTableName(), tableRegion.getStartKey()));
		}
		return regions;
	}

	private void submitAndWait(HTableInterface htable,
										 final AtomicLong rowsImported,
										 Multimap<HRegionLocation, BlockLocation> regionToBlockMap) throws IOException {
		/*
		 * This will submit our BlockLocations out to all our available Regions asynchronously, then
		 * wait for all of them to respond back with success before returning.
		 */
		final CountDownLatch latch = new CountDownLatch(regionToBlockMap.size());
		for (HRegionLocation region : regionToBlockMap.keySet()) {
			HRegionInfo regionInfo = region.getRegionInfo();
			byte[] startRow = regionInfo.getStartKey();
			byte[] endRow = regionInfo.getEndKey();
			if (endRow.length > 0)
				BytesUtil.decrementAtIndex(endRow, endRow.length - 1);//ensure that we are inside the region fully

			final Collection<BlockLocation> blockLocs = regionToBlockMap.get(region);
			try {
				htable.coprocessorExec(SpliceImportProtocol.class, startRow, endRow,
						new Batch.Call<SpliceImportProtocol, Long>() {

							@Override
							public Long call(SpliceImportProtocol instance)
									throws IOException {
								return instance.doImport(blockLocs, context);
							}
						}, new Batch.Callback<Long>() {

							@Override
							public void update(byte[] region, byte[] row,
																 Long result) {
								rowsImported.addAndGet(result);
								latch.countDown();
							}
						}
				);
			} catch (Throwable e) {
				throw new IOException(e);
			}
		}

		//all of this round's imports have been submitted, now just need to wait for them to finish
		try {
			latch.await();
		} catch (InterruptedException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Unexpected interruption occurred", e);
		}
	}

	private HRegionLocation getLeastLoadedRegion(
			Map<HRegionLocation, Integer> taskSizeMap) {
		/*
		 * gets the smallest region based on load
		 */
		HRegionLocation smallLoc = null;
		int smallCount = Integer.MAX_VALUE;
		for (HRegionLocation loc : taskSizeMap.keySet()) {
			if (smallCount < taskSizeMap.get(loc)) {
				smallCount = taskSizeMap.get(loc);
				smallLoc = loc;
			}
		}
		return smallLoc;
	}

	private void populateRegionLocationMatches(BlockLocation location, List<HRegionLocation> regions,
																		 Map<HRegionLocation, Integer> taskSizeMap)
			throws IOException {
		/*
		 * populates a load map for all the replicas of the given BlockLocation
		 */
		String[] blockHosts = location.getHosts();
		for (HRegionLocation region : regions) {
			String host = region.getHostname();
			for (String blockHost : blockHosts) {
				if (blockHost.equals(host)) {
					if (taskSizeMap.get(region) == null)
						taskSizeMap.put(region, 1);
					else
						taskSizeMap.put(region, taskSizeMap.get(region) + 1);
				}
			}
		}
	}

}