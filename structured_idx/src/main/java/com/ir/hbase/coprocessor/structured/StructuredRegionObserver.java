package com.ir.hbase.coprocessor.structured;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionScannerUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.BaseZkRegionObserver;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.coprocessor.index.IndexUtils;
import com.ir.hbase.txn.coprocessor.region.TransactionalRegionObserver;
import com.ir.hbase.txn.coprocessor.region.TxnUtils;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants.TableEnv;

public class StructuredRegionObserver extends BaseZkRegionObserver {
	private static Logger LOG = Logger.getLogger(StructuredRegionObserver.class);
	private boolean tableEnvMatch = false;
	private boolean otherTable = false;
	private Map<byte[], Column> columnsByName = new ConcurrentHashMap<byte[], Column>();	
	private Map<byte[], Column> columnsDeleted = new ConcurrentHashMap<byte[], Column>();

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Starting the CoProcessor " + StructuredRegionObserver.class);
		if (LOG.isTraceEnabled())
			LOG.trace("Starting the CoProcessor ");
		if(TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.OTHER_TABLES)) {
			initZooKeeper(e);
			readSchemaFromZoo();
			otherTable = true;
		}
		if(TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.OTHER_TABLES)
				|| TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.INDEX_TABLE))
			tableEnvMatch = true;
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("Stopping the CoProcessor ");
		super.stop(e);
	}
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		if (otherTable && StructuredUtils.isStructuredChngeEnv(put)) {
			StructuredUtils.changeStructure(schemaPath, tableName, put, rzk, e);
		}
		super.prePut(e, put, edit, writeToWAL);
	}
	
	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e,Get get, List<KeyValue> results) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preGet in CoProcessor " + TransactionalRegionObserver.class);
		if(!tableEnvMatch) {
			return;
		}
		if (!columnsDeleted.isEmpty()) {
			get = StructuredUtils.wrapGetWithDeleteColumnFilter(get, columnsDeleted);
		}
		
	}
	
	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preScannerOpen in CoProcessor " + StructuredRegionObserver.class);
		if(!tableEnvMatch) {
			return s;
		}
		if (!columnsDeleted.isEmpty()) {
			scan = StructuredUtils.wrapWithDeleteColumnFilter(scan, columnsDeleted);
			return RegionScannerUtil.getRegionScanner(region, scan, new ArrayList<KeyValueScanner>());
		}
		return s;
	}
	private void readSchemaFromZoo() {
		if (LOG.isDebugEnabled())
			LOG.debug("Read schema from ZooKeeper for table " + tableName);
		if (StructuredUtils.hasStructure(schemaPath, tableName, rzk)) {
			try {
				for (String family : rzk.getChildren(SchemaUtils.getTablePath(schemaPath, tableName), new TableWatcher())) {
					for (String column : rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, tableName, family), new FamilyWatcher(family))) {
						byte[] bytes = KeyValue.makeColumn(Bytes.toBytes(family), Bytes.toBytes(column));
						columnsByName.put(bytes, Column.toColumn(Bytes.toString(rzk.getData(SchemaUtils.getColumnPath(schemaPath, tableName, family, column), new OldColumnWatcher(bytes), null))));
					}
				}
			} catch (KeeperException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	private class TableWatcher implements Watcher {

		public TableWatcher() {
			
		}
		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled())
				LOG.debug("Table watcher triggered in " + TransactionalRegionObserver.class);
			if (event.getType().equals(EventType.NodeChildrenChanged)) {
				try {
					for (String family : rzk.getChildren(SchemaUtils.getTablePath(schemaPath, tableName), this)) {
						if (!StructuredUtils.hasFamily(family, columnsByName)) {
							rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, tableName, family), new FamilyWatcher(family));
						}
					}
				} catch (KeeperException e1) {
					e1.printStackTrace();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	private class FamilyWatcher implements Watcher {
		private String family;

		public FamilyWatcher(String family) {
			this.family = family;
		}
		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled())
				LOG.debug("Family watcher triggered in " + TransactionalRegionObserver.class);
			if (event.getType().equals(EventType.NodeChildrenChanged)) {
				try {
					List<String> columns = rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, tableName, family), this);
					for (String column : columns) {
						byte[] bytes = KeyValue.makeColumn(Bytes.toBytes(family), Bytes.toBytes(column));
						if (!columnsByName.containsKey(bytes)) {
							columnsByName.put(bytes, Column.toColumn(Bytes.toString(rzk.getData(schemaPath + SchemaConstants.PATH_DELIMITER + tableName + SchemaConstants.PATH_DELIMITER + family + SchemaConstants.PATH_DELIMITER + column, new OldColumnWatcher(bytes), null))));
							columnsDeleted.remove(bytes);
						}
					}
				} catch (KeeperException e1) {
					if (e1.code().equals(Code.NONODE))
						LOG.debug("Node " + SchemaUtils.getFamilyPath(schemaPath, tableName, family) + " was deleted.");
					else 
						e1.printStackTrace();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
	private class OldColumnWatcher implements Watcher {
		private byte[] column; //family:column

		public OldColumnWatcher(byte[] column) {
			this.column = column;
		}
		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled())
				LOG.debug("Old column watcher triggered in " + StructuredRegionObserver.class);
			if (event.getType().equals(EventType.NodeDeleted)) {
				columnsDeleted.put(column, columnsByName.get(column));
				columnsByName.remove(column);
			}
		}
	}
}