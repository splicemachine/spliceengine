package com.ir.hbase.coprocessor.structured;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import com.ir.constants.SpliceConstants;
import com.ir.constants.SpliceConstants;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.index.mapreduce.DictionaryMapReduceUtil;

public class StructuredUtils extends SpliceConstants {
	private static Logger LOG = Logger.getLogger(StructuredUtils.class);
	public static boolean hasStructure(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " has structure definition in ZooKeeper.");
		try {
			return rzk.exists(SchemaUtils.getTablePath(schemaPath, tableName), false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}


	public static Get wrapGetWithDeleteColumnFilter(final Get get, final Map<byte[], Column> columnsDeleted) {
		if (LOG.isDebugEnabled())
			LOG.debug("Wrap Get with delete filter.");
		if (get.getFilter() == null) {
			get.setFilter(getDeleteColumnFilter(columnsDeleted));
			return get;
		}
		FilterList wrappedFilter = new FilterList(Arrays.asList(getDeleteColumnFilter(columnsDeleted), get.getFilter()));
		get.setFilter(wrappedFilter);
		return get;
	}

	public static Scan wrapWithDeleteColumnFilter(final Scan scan, final Map<byte[], Column> columnsDeleted) {
		if (LOG.isDebugEnabled())
			LOG.debug("Wrap Scan with delete filter.");
		if (scan.getFilter() == null) {
			scan.setFilter(getDeleteColumnFilter(columnsDeleted));
			return scan;
		}
		FilterList wrappedFilter = new FilterList(Arrays.asList(getDeleteColumnFilter(columnsDeleted), scan.getFilter()));
		scan.setFilter(wrappedFilter);
		return scan;
	}

	public static Filter getDeleteColumnFilter(final Map<byte[], Column> columnsDeleted) {
		if (LOG.isDebugEnabled())
			LOG.debug("Generate delete filter.");
		FilterBase deleteFilter = new FilterBase() {
			private boolean rowFiltered = false;
			@Override
			public void reset() {
				rowFiltered = false;
			}
			@Override
			public boolean hasFilterRow() {
				return true;
			}
			@Override
			public void filterRow(final List<KeyValue> kvs) {
				for (Iterator<KeyValue> itr = kvs.iterator(); itr.hasNext();) {
					KeyValue included = applyDeleteColumn(itr.next());
					if (null == included) {
						itr.remove();
					}
				}
				rowFiltered = kvs.isEmpty();
			}
			public KeyValue applyDeleteColumn(KeyValue kv) {
				for (byte[] column : columnsDeleted.keySet()) {
					if (Bytes.equals(KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()), column))
						return null;
				}
				return kv;
			}
			@Override
			public boolean filterRow() {
				return rowFiltered;
			}
			@Override
			public void write(final DataOutput out) throws IOException {
			}
			@Override
			public void readFields(final DataInput in) throws IOException {
			}
		};
		return deleteFilter;
	}

	public static void createStructureNodes(TableStructure ts, String schemaPath, String tableName, HTableDescriptor desc, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Initialize structure nodes in ZooKeeper for table " + tableName);
		//Create table node
		createTableNode(schemaPath, tableName, rzk);
		//Create default family node anyway
		createFamilyNode(schemaPath, tableName, SpliceConstants.DEFAULT_FAMILY, rzk);
		//add default family if not exist
		if (!desc.hasFamily(SpliceConstants.DEFAULT_FAMILY_BYTES))
			desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
					SpliceConstants.DEFAULT_VERSIONS,
					SpliceConstants.DEFAULT_COMPRESSION,
					SpliceConstants.DEFAULT_IN_MEMORY,
					SpliceConstants.DEFAULT_BLOCKCACHE,
					SpliceConstants.DEFAULT_TTL,
					SpliceConstants.DEFAULT_BLOOMFILTER));
		if (ts != null) { //user defined table table structure
			if (ts.hasFamilies()) {
				for (Family family : ts.getFamilies()) {
					if (!desc.hasFamily(family.getFamilyName().getBytes())) {
						desc.addFamily(new HColumnDescriptor(family.getFamilyName()));
					}
					//Create family node
					createFamilyNode(schemaPath, tableName, family.getFamilyName(), rzk);
					for (Column col : family.getColumns()) {
						//Create column node
						createColumnNode(schemaPath, tableName, family.getFamilyName(), col.getColumnName(), Bytes.toBytes(col.toJSon()), rzk);
					}
				}
			}
		} else { //no user defined structure, create with table descriptor
			if (desc.getFamilies().isEmpty()) { //Create default family node
				createFamilyNode(schemaPath, tableName, SpliceConstants.DEFAULT_FAMILY, rzk);
			} else { //Create user-defined family node
				for (HColumnDescriptor family : desc.getFamilies()) {
					createFamilyNode(schemaPath, tableName, family.getNameAsString(), rzk);
				}
			}
		}
	}

	public static void changeStructure(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			if (Bytes.equals(put.getRow(), SpliceConstants.ADD_COLUMN_BYTE)) {
				addColumn(schemaPath, tableNameStr, put, rzk, e.getEnvironment().getConfiguration());
				e.bypass();
				e.complete();
			} else if (Bytes.equals(put.getRow(), SpliceConstants.DELETE_COLUMN_BYTE)) {
				deleteColumn(schemaPath, tableNameStr, put, rzk, e.getEnvironment().getConfiguration());
				e.bypass();
				e.complete();
			} else {

			}
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		} catch (KeeperException ex) {
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	public static void addColumn(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, Configuration conf) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Adding column to table " + tableNameStr);
		String columnStr = Bytes.toString(put.getAttribute(SERIALIZED_COLUMN));
		if (columnStr == null)
			throw new RuntimeException("Try to add a column without serialized column definition.");
		Column columnIns = Column.toColumn(columnStr);
		if (hasColumn(schemaPath, tableNameStr, columnIns.getFamily(), columnIns.getColumnName(), rzk))
			throw new RuntimeException("Try to add an existing column.");
		else if (!hasFamily(schemaPath, tableNameStr, columnIns.getFamily(), rzk))
			addNewFamilyToTable(tableNameStr, columnIns.getFamily(), conf);
		SchemaUtils.createWithParent(rzk, SchemaUtils.getColumnPath(schemaPath, tableNameStr, columnIns.getFamily(), columnIns.getColumnName()), Bytes.toBytes(columnIns.toJSon()));
	}

	public static void deleteColumn(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, Configuration conf) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Deleting column from table " + tableNameStr);
		String columnStr = Bytes.toString(put.getAttribute(SERIALIZED_COLUMN));
		if (columnStr == null)
			throw new RuntimeException("Try to delete a column without serialized column definition.");
		Column columnIns = Column.toColumn(columnStr);
		if (!hasColumn(schemaPath, tableNameStr, columnIns.getFamily(), columnIns.getColumnName(), rzk))
			throw new RuntimeException("Try to delete an unexisting column.");
		rzk.delete(SchemaUtils.getColumnPath(schemaPath, tableNameStr, columnIns.getFamily(), columnIns.getColumnName()), -1);
		if (rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, tableNameStr, columnIns.getFamily()), false).isEmpty()) {
			rzk.delete(SchemaUtils.getFamilyPath(schemaPath, tableNameStr, columnIns.getFamily()), -1);
		}
		DictionaryMapReduceUtil.deleteColumn(Bytes.toBytes(tableNameStr), columnIns, HConstants.LATEST_TIMESTAMP, conf);
	}

	public static boolean hasFamily(String schemaPath, String tableName, String family, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " has family " + family + " in zookeeper.");
		try {
			return rzk.exists(SchemaUtils.getFamilyPath(schemaPath, tableName, family), false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean hasColumn(String schemaPath, String tableName, String family, String column, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " has column " + column + " in zookeeper.");
		try {
			return rzk.exists(SchemaUtils.getColumnPath(schemaPath, tableName, family, column), false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean hasTable(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " exists in zookeeper.");
		try {
			return rzk.exists(SchemaUtils.getTablePath(schemaPath, tableName), false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean hasFamily(String family, Map<byte[], Column> columnsByName) {
		for(byte[] bytes : columnsByName.keySet()) {
			byte[][] column = KeyValue.parseColumn(bytes);
			if (Bytes.equals(Bytes.toBytes(family), column[0]))
				return true;
		}
		return false;
	}

	public static void createTableNode(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		if (!hasTable(schemaPath, tableName, rzk)) {
			try {
				rzk.create(SchemaUtils.getTablePath(schemaPath, tableName), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void createFamilyNode(String schemaPath, String tableName, String family, RecoverableZooKeeper rzk) {
		if (LOG.isTraceEnabled())
			LOG.trace("Creating family Node " + family);
		if (!hasFamily(schemaPath, tableName, family, rzk)) {
			try {
				rzk.create(SchemaUtils.getFamilyPath(schemaPath, tableName, family), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void createColumnNode(String schemaPath, String tableName, String family, String column, byte[] value, RecoverableZooKeeper rzk) {
		if (LOG.isTraceEnabled())
			LOG.trace("Creating column Node " + family + ":" + column);
		if (!hasColumn(schemaPath, tableName, family, column, rzk)) {
			try {
				rzk.create(SchemaUtils.getColumnPath(schemaPath, tableName, family, column), value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void addNewFamilyToTable(String tableName, String family, Configuration conf) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.addColumn(tableName, new HColumnDescriptor(family));
			admin.enableTable(tableName);
		} catch (IOException e) {
			LOG.debug("Failed to add new family to table " + tableName);
			e.printStackTrace();
		} finally {
		}
	}
	public static boolean isStructuredChngeEnv(Put put) {
		return Bytes.equals(put.getRow(), SpliceConstants.ADD_COLUMN_BYTE) || Bytes.equals(put.getRow(), SpliceConstants.DELETE_COLUMN_BYTE);
	} 
	public static void deleteStructuredTablePathRecursively(String schemaPath, String tableName, ZooKeeperWatcher zkw) {
		RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
		try {
			if (rzk.getChildren(SchemaUtils.getTablePath(schemaPath, tableName), false) != null)
				ZKUtil.deleteNodeRecursively(zkw, SchemaUtils.getTablePath(schemaPath, tableName));
			else
				rzk.delete(SchemaUtils.getTablePath(schemaPath, tableName), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
