package com.ir.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.coprocessor.index.IndexUtils;
import com.ir.hbase.coprocessor.structured.StructuredUtils;
import com.ir.hbase.txn.logger.LogConstants;

public class SchemaManager extends SchemaConstants {
	private static Logger LOG = Logger.getLogger(SchemaManager.class);
	private String schemaPath;
	private HBaseAdmin admin;
	private Configuration config;
	private RecoverableZooKeeper rzk;
	private HTablePool tablePool;
	private ZooKeeperWatcher zkw;

	@SuppressWarnings("deprecation")
	public SchemaManager(HBaseAdmin admin) throws IOException {
		this.config = admin.getConfiguration();
		schemaPath = config.get(SchemaConstants.SCHEMA_PATH_NAME,SchemaConstants.DEFAULT_SCHEMA_PATH);
		this.admin = admin;
		this.zkw = admin.getConnection().getZooKeeperWatcher();
		this.rzk = zkw.getRecoverableZooKeeper();
		tablePool = new HTablePool();
	}

	public boolean hasIndex(String tableName) {
		return IndexUtils.hasIndexes(tableName, tableName, rzk);
	}

	public boolean hasIndex(String tableName, String indexName) {
		return IndexUtils.hasIndex(schemaPath, tableName, indexName, rzk);
	}

	public boolean hasColumn(String tableName, String family, String column) {
		return StructuredUtils.hasColumn(schemaPath, tableName, family, column, rzk);
	}
	/**
	 * Read table structure from zoo.
	 */
	public TableStructure getTableStructure(String tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's reading table structure from zoo for table " + tableName);
		try {
			if (rzk.exists(SchemaUtils.getTablePath(schemaPath, tableName), false) != null) {
				TableStructure ts = new TableStructure(tableName);
				List<String> families = rzk.getChildren(SchemaUtils.getTablePath(schemaPath, tableName), false);
				for (String family : families) {
					Family familyIns = new Family(family);
					List<String> columns = rzk.getChildren(SchemaUtils.getFamilyPath(schemaPath, tableName, family), false);
					for (String column : columns) {
						familyIns.addColumn(Column.toColumn(Bytes.toString(rzk.getData(SchemaUtils.getColumnPath(schemaPath, tableName, family, column), false, null))));
					}
					ts.addFamily(familyIns);
				}
				return ts;
			} else {
				throw new IOException("No table structure defined in ZooKeeper for table " + tableName);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public IndexTableStructure getIndexTableStructure(String tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's reading index table structure from zoo for table " + tableName);
		try {
			if (rzk.exists(SchemaUtils.getIndexTablePath(schemaPath, tableName), false) != null) {
				IndexTableStructure its = new IndexTableStructure(tableName);
				List<String> indexes = rzk.getChildren(SchemaUtils.getIndexTablePath(schemaPath, tableName), false);
				for (String index : indexes) {
					Index indexIns = Index.toIndex(Bytes.toString(rzk.getData(SchemaUtils.getIndexPath(schemaPath, tableName, index), false, null)));
					its.addIndex(indexIns);
				}
				return its;
			} else {
				throw new IOException("No index table definition in ZooKeeper for table " + tableName);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public byte[] getAvailableFamily(String tableName) {
		try {
			return admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies()[0].getName();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void addIndexToTable(String tableName, Index index) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's adding index " + index.getIndexName() + " to table " + tableName);
		Put put = new Put(ADD_INDEX_BYTE);
		put.add(getAvailableFamily(tableName), ADD_INDEX_BYTE, ADD_INDEX_BYTE);
		put.setAttribute(SERIALIZED_INDEX, Bytes.toBytes(index.toJSon()));
		try {
			HTableInterface table = tablePool.getTable(tableName);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteIndexFromTable(String tableName, String indexName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's deleting index " + indexName + " from table " + tableName);
		Put put = new Put(DELETE_INDEX_BYTE);
		put.add(getAvailableFamily(tableName), DELETE_INDEX_BYTE, DELETE_INDEX_BYTE);
		put.setAttribute(SERIALIZED_INDEX, Bytes.toBytes(indexName));
		try {
			HTableInterface table = tablePool.getTable(tableName);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void addColumnToTable(String tableName, Column column) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's adding column " + column.getColumnName() + " to table " + tableName);
		Put put = new Put(ADD_COLUMN_BYTE);
		put.add(getAvailableFamily(tableName), ADD_COLUMN_BYTE, ADD_COLUMN_BYTE);
		put.setAttribute(SERIALIZED_COLUMN, Bytes.toBytes(column.toJSon()));
		try {//must acess index table to use schema editor
			HTableInterface table = tablePool.getTable(tableName);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void deleteColumnFromTable(String tableName, Column column) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("SchemaManager's deleting column " + column.getColumnName() + " from table " + tableName);
		Put put = new Put(DELETE_COLUMN_BYTE);
		put.add(getAvailableFamily(tableName), DELETE_COLUMN_BYTE, DELETE_COLUMN_BYTE);
		put.setAttribute(SERIALIZED_COLUMN, Bytes.toBytes(column.toJSon()));
		try {
			HTableInterface table = tablePool.getTable(tableName);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Clean child nodes of transaction, log. 
	 */
	public void cleanTransactionNodes() {
		String transactionPath = config.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH);
		String logPath =  config.get(LogConstants.LOG_PATH_NAME,LogConstants.DEFAULT_LOG_PATH);
		String txnLogPath = logPath + LogConstants.TXN_LOG_SUBPATH;
		String regionLogPath = logPath + LogConstants.REGION_LOG_SUBPATH;
		try {
			ZKUtil.deleteChildrenRecursively(zkw, transactionPath);
			ZKUtil.deleteChildrenRecursively(zkw, txnLogPath);
			ZKUtil.deleteChildrenRecursively(zkw, regionLogPath);
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Clean child nodes of schema. 
	 */
	public void cleanSchemaNodes() {
		try {
			ZKUtil.deleteChildrenRecursively(zkw, schemaPath);
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
}
