package com.ir.hbase.coprocessor.index;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.BaseZkRegionObserver;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.txn.coprocessor.region.TxnUtils;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants.TableEnv;

public class IndexRegionObserver extends BaseZkRegionObserver {
	private static Logger LOG = Logger.getLogger(IndexRegionObserver.class);
	private boolean tableEnvMatch = false;
	private Map<String, Index> indexesByName = new ConcurrentHashMap<String, Index>();
	private HTablePool tablePool;
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		LOG.debug("Starting the CoProcessor " + IndexRegionObserver.class);
		if (LOG.isTraceEnabled())
			LOG.trace("Starting the CoProcessor ");
		if(TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.OTHER_TABLES)) {
			initZooKeeper(e);
			readIndexFromZoo();
			tableEnvMatch = true;
			tablePool = new HTablePool(e.getConfiguration(), Integer.MAX_VALUE);
		}
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("Stopping the CoProcessor ");
		super.stop(e);
		if(tableEnvMatch) {
			tablePool.close();
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("prePut in CoProcessor " + IndexRegionObserver.class);
			LOG.debug(tableEnvMatch);
			LOG.debug(indexesByName.size());
		}
		if (tableEnvMatch) {
			if (!indexesByName.isEmpty()) {
					HTableInterface indexTable = tablePool.getTable(Bytes.toBytes(region.getRegionInfo().getTableNameAsString() + SchemaConstants.INDEX));
					IndexUtils.updateIndexes(indexTable, put, region, indexesByName, null);
					indexTable.close();
			}
			if (IndexUtils.isIndexChngeEnv(put))
				IndexUtils.changeIndex(schemaPath, tableName, put, rzk, e);
		}
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
			Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preDelete in CoProcessor " + IndexRegionObserver.class);
		if (tableEnvMatch && !indexesByName.isEmpty()) {
			NavigableSet<Column> neededColumns = IndexUtils.getColumnsForIndexes(indexesByName.values());
			//Get old result
			Result oldRow = IndexUtils.getOldResult(delete, region, null);
			SortedMap<byte[], byte[]> oldColumnValues = IndexUtils.convertToValueMap(oldRow);
			//Get new result
			Result newRow = IndexUtils.applyDelete(delete, oldRow);
			SortedMap<byte[], byte[]> newColumnValues = IndexUtils.convertToValueMap(newRow);
			HTableInterface indexTable = tablePool.getTable(Bytes.toBytes(region.getRegionInfo().getTableNameAsString() + SchemaConstants.INDEX));
			IndexUtils.deleteOnIndexTable(indexTable, delete, indexesByName, oldColumnValues, newColumnValues);
			indexTable.close();
			super.preDelete(e, delete, edit, writeToWAL);
		}
	}

	private void readIndexFromZoo() throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Read index definition from zoo for table " + tableName);
		if (LOG.isTraceEnabled())
			LOG.trace("Read index definition from zoo for table " + tableName + " has an Index");
		try {
			for (String child : rzk.getChildren(SchemaUtils.getIndexTablePath(schemaPath, tableName), new NexIndexWatcher())) {
				LOG.trace("Read index definition from zoo for index " + child);
				indexesByName.put(child, Index.toIndex(Bytes.toString(rzk.getData(SchemaUtils.getIndexPath(schemaPath, tableName, child), new OldIndexWatcher(child), null))));
			}
		} catch (KeeperException e) {
			e.printStackTrace();
			throw new IOException("Keeper Exception in readIndexFromZoo: " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IOException("Keeper Exception in readIndexFromZoo: " + e.getMessage());
		}
	}

	private class NexIndexWatcher implements Watcher {		
		public NexIndexWatcher() {

		}
		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled())
				LOG.debug("New index watcher triggered in CoProcessor " + IndexRegionObserver.class);
			if (event.getType().equals(EventType.NodeChildrenChanged)) {
				try {
					for (String child : rzk.getChildren(schemaPath + SchemaConstants.PATH_DELIMITER + tableName + SchemaConstants.INDEX, this)) {
						if (!indexesByName.containsKey(child)) {
							if (LOG.isDebugEnabled())
								LOG.debug("New index found in CoProcessor " + IndexRegionObserver.class);
							indexesByName.put(child, Index.toIndex(Bytes.toString(rzk.getData(schemaPath + SchemaConstants.PATH_DELIMITER + tableName + SchemaConstants.INDEX + SchemaConstants.PATH_DELIMITER + child, new OldIndexWatcher(child), null))));
						}
					}
				} catch (KeeperException e) {
					e.printStackTrace();
					LOG.error("New Index Watcher exception: " + e.getMessage());
				} catch (InterruptedException e) {
					e.printStackTrace();
					LOG.error("New Index Watcher exception: " + e.getMessage());
				}
			}
		}
	}
	private class OldIndexWatcher implements Watcher {
		private String indexPath;
		public OldIndexWatcher(String indexPath) {
			this.indexPath = indexPath;
		}
		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled())
				LOG.debug("Old index watcher triggered in CoProcessor " + IndexRegionObserver.class);
			if (event.getType().equals(EventType.NodeDeleted)) {
				indexesByName.remove(indexPath);
			}
		}
	}
}
