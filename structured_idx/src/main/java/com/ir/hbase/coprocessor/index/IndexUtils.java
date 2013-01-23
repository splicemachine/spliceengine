package com.ir.hbase.coprocessor.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import com.ir.constants.HBaseConstants;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.constants.bytes.BytesUtil;
import com.ir.constants.bytes.SortableByteUtil;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.index.mapreduce.DictionaryMapReduceUtil;

public class IndexUtils extends SchemaConstants {
	private static Logger LOG = Logger.getLogger(IndexUtils.class);
	/**
	 * Check if a table has index definition in zoo. 
	 */
	public static boolean hasIndexes(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " has any index definition in zoo.");
		try {
			if (hasIndexTable(schemaPath, tableName, rzk) && !rzk.getChildren(SchemaUtils.getIndexTablePath(schemaPath, tableName), false).isEmpty()) {
				return true;
			}
		} catch (KeeperException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());
		}
		return false;
	}

	public static boolean hasIndexTable(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		try {
			if (rzk.exists(SchemaUtils.getIndexTablePath(schemaPath, tableName), false) != null) {
				return true;
			}
		} catch (KeeperException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());
		}
		return false;
	}

	/**
	 * Check if a table has index in zoo with given name 
	 */
	public static boolean hasIndex(String schemaPath, String tableName, String indexName, RecoverableZooKeeper rzk) {
		if (LOG.isDebugEnabled())
			LOG.debug("Check if table " + tableName + " has index definition for " + indexName + " in zoo.");
		try {
			return rzk.exists(SchemaUtils.getIndexPath(schemaPath, tableName, indexName), false) != null;
		} catch (KeeperException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOG.error("Keeper Exception hasIndex " + e.getMessage());

		}
		return false;
	}

	public static void createIndexTableNode(String schemaPath, String tableName, RecoverableZooKeeper rzk) {
		if (!hasIndexTable(schemaPath, tableName, rzk)) {
			try {
				rzk.create(SchemaUtils.getIndexTablePath(schemaPath, tableName), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void createIndexNode(String schemaPath, String tableName, String indexName, byte[] value, RecoverableZooKeeper rzk) {
		if (!hasIndex(schemaPath, tableName, indexName, rzk)) {
			try {
				rzk.create(SchemaUtils.getIndexPath(schemaPath, tableName, indexName), value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static SortedMap<byte[], byte[]> convertToValueMap(final Result result) {
		SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);

		if (result == null || result.raw() == null) {
			return currentColumnValues;
		}
		List<KeyValue> list = result.list();
		if (list != null) {
			for (KeyValue kv : result.list()) {
				currentColumnValues.put(
						KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()),
						kv.getValue());
			}
		}
		return currentColumnValues;
	}

	public static void updateIndexes(final HTableInterface indexTable, final Put put, final HRegion region, Map<String, Index> indexMap, final Integer lockId)
			throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Start update applicable Indexes");
		//Get old result
		Result oldResult = getOldResult(put, region, lockId);
		//Get columns that matters from put
		NavigableMap<byte[], byte[]> newColumnValues = getColumnsFromPut(put);
		//Filter indexes based on new column values
		AddOldResultToNew(oldResult, newColumnValues);

		SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldResult);

		for (Index index : indexMap.values()) { //Update indexes
			updateIndex(indexTable, index, put, newColumnValues, oldColumnValues);
		}
	}

	public static void AddOldResultToNew(Result oldResult, NavigableMap<byte[], byte[]> newColumnValues) {
		if (oldResult != null && oldResult.raw() != null) {
			for (KeyValue oldKV : oldResult.raw()) {
				byte[] column = KeyValue.makeColumn(oldKV.getFamily(),
						oldKV.getQualifier());
				if (!newColumnValues.containsKey(column)) {
					newColumnValues.put(column, Arrays.copyOfRange(oldKV.getValue(), 0, oldKV.getValue().length));
				}
			}
		}
	}

	public static NavigableSet<Column> getColumnsForIndexes(final Collection<Index> indexes) {
		NavigableSet<Column> neededColumns = new TreeSet<Column>();
		for (Index index : indexes) {
			for (Column col : index.getAllColumns()) {
				neededColumns.add(col);
			}
		}
		return neededColumns;
	}

	public static NavigableMap<byte[], byte[]> getColumnsFromPut(final Put put) {
		NavigableMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);
		for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
			for (KeyValue kv : familyPuts) {
				columnValues.put(
						KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()),
						kv.getValue());
			}
		}
		return columnValues;
	}

	public static void updateIndex(final HTableInterface indexTable, final Index index, final Put put,
			final NavigableMap<byte[], byte[]> newColumnValues,
			final SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Updating index " + index.getIndexName());
		Delete indexDelete = makeDeleteToRemoveOldIndexEntry(index, put.getRow(), oldColumnValues);
		Put indexPut = makeIndexUpdate(index, put.getRow(), newColumnValues);
		if (put.getAttribute(TxnConstants.TRANSACTION_ID) != null) {
			if (indexDelete != null)
				indexDelete.setAttribute(TxnConstants.TRANSACTION_ID, put.getAttribute(TxnConstants.TRANSACTION_ID));
			indexPut.setAttribute(TxnConstants.TRANSACTION_ID, put.getAttribute(TxnConstants.TRANSACTION_ID));
		}

		if (indexDelete != null && !Bytes.equals(indexDelete.getRow(), indexPut.getRow())) {
			indexTable.delete(indexDelete);
		}
		indexTable.put(indexPut);
	}

	public static boolean possiblyAppliesToIndex(final Index index,
			final Put put) {
		for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
			for (KeyValue kv : familyPuts) {
				if (index.containsColumn(new Column(Bytes.toString(kv.getFamily()),
						Bytes.toString(kv.getQualifier()))))
					return true;
			}
		}
		return false;
	}

	public static Delete makeDeleteToRemoveOldIndexEntry(final Index index, final byte[] row,
			final SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
		if (oldColumnValues.isEmpty()) {
			return null;
		}

		byte[] oldIndexRow = index.createIndexKey(row, oldColumnValues);
		return new Delete(oldIndexRow);
	}

	public static Put makeIndexUpdate(final Index index, final byte[] row, final SortedMap<byte[], byte[]> columnValues)
			throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Make index update with index " + index.getIndexName());
		Put update = new Put(index.createIndexKey(row, columnValues));
		update.add(INDEX_BASE_FAMILY_BYTE, INDEX_BASE_ROW_BYTE, row);

		for (Column col : index.getAllColumns()) {
			byte[] val = columnValues.get(col.makeColumn());
			if (val == null) {
				if (LOG.isDebugEnabled())
					LOG.debug("Got Null column value when updating [" + col.getColumnName() + "]");
				update.add(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getColumnName()), new byte[0]);
			} else {
				if (LOG.isDebugEnabled())
					LOG.debug("Got column value " + Bytes.toString(val) + " when updating [" + col.getColumnName() + "]");
				update.add(Bytes.toBytes(col.getFamily()), Bytes.toBytes(col.getColumnName()), val);
			}
		}
		return update;
	}

	public static boolean doesApplyToIndex(final Index index,
			final SortedMap<byte[], byte[]> columnValues) {
		for (Column neededColumn : index.getIndexColumns()) {
			if (!columnValues.containsKey(neededColumn.makeColumn()))
				return false;
		}
		return true;
	}

	public static Result getOldResult(final Mutation mutation, final HRegion region, final Integer lockId) throws IOException {
		Get oldGet = new Get(mutation.getRow());
		if (mutation.getAttribute(TxnConstants.TRANSACTION_ID) != null) {
			oldGet.setAttribute(TxnConstants.TRANSACTION_ID, mutation.getAttribute(TxnConstants.TRANSACTION_ID));
			oldGet.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
		} else {
			oldGet.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
		}
		return region.get(oldGet, lockId);
	}

	public static void deleteOnIndexTable(HTableInterface indexTable, Delete delete, Map<String, Index> indexesByName, SortedMap<byte[], byte[]> oldColumnValues, SortedMap<byte[], byte[]> newColumnValues) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Perform delete on index table " + Bytes.toString(indexTable.getTableName()));
		for (Index index : indexesByName.values()) {
			Delete indexDelete = makeDeleteToRemoveOldIndexEntry(index, delete.getRow(), oldColumnValues);
			Put indexPut = null;
			if (!newColumnValues.isEmpty()) {
				indexPut = makeIndexUpdate(index, delete.getRow(), newColumnValues);
			}
			if (delete.getAttribute(TxnConstants.TRANSACTION_ID) != null) {
				if (indexDelete != null)
					indexDelete.setAttribute(TxnConstants.TRANSACTION_ID, delete.getAttribute(TxnConstants.TRANSACTION_ID));
				if (indexPut != null)
					indexPut.setAttribute(TxnConstants.TRANSACTION_ID, delete.getAttribute(TxnConstants.TRANSACTION_ID));
			}
			if (indexDelete != null && (newColumnValues.isEmpty() || !Bytes.equals(indexDelete.getRow(), indexPut.getRow()))) { //delete whole row
				indexTable.delete(indexDelete);
			} else if (indexDelete != null) { //delete partial
				deletePartialRow(indexTable, delete, index, indexDelete);
			}
			if (indexPut != null) {
				indexTable.put(indexPut);
			}
		}
	}

	public static void deletePartialRow(HTableInterface indexTable, Delete delete, Index index, Delete indexDelete) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Delete partial row in index table " + Bytes.toString(indexTable.getTableName()));
		for (Column col : index.getAddColumns()) {
			List<KeyValue> famDeletes = delete.getFamilyMap().get(Bytes.toBytes(col.getFamily()));
			if (famDeletes != null) {
				for (KeyValue kv : famDeletes) {
					if (Bytes.equals(col.makeColumn(), KeyValue.makeColumn(kv.getFamily(), kv.getQualifier()))) {
						Delete columnDelete = new Delete(indexDelete.getRow());
						if (delete.getAttribute(TxnConstants.TRANSACTION_ID) != null) {
							columnDelete.setAttribute(TxnConstants.TRANSACTION_ID, delete.getAttribute(TxnConstants.TRANSACTION_ID));
						}
						columnDelete.deleteColumns(kv.getFamily(),
								kv.getQualifier());
						indexTable.delete(columnDelete);
					}
				}
			}
		}
	}

	public static Result applyDelete(Delete delete, Result input) {
		if (input == null || input.list() == null) 
			return input;
		List<KeyValue> result = new ArrayList<KeyValue>(input.list());
		for (Iterator<KeyValue> itr = result.iterator(); itr.hasNext();) {
			KeyValue included = applyDelete(delete, itr.next());
			if (null == included) {
				itr.remove();
			}
		}
		return new Result(result);
	}

	public static KeyValue applyDelete(final Delete delete, final KeyValue kv) {
		// Skip if delete should not apply
		if (kv.getTimestamp() > delete.getTimeStamp()) {
			return kv;
		}
		// Whole-row delete
		if (delete.isEmpty()) {
			return null;
		}
		for (Entry<byte[], List<KeyValue>> deleteEntry : delete.getFamilyMap().entrySet()) {
			byte[] family = deleteEntry.getKey();
			if (!Bytes.equals(kv.getFamily(), family)) {
				return kv;
			}
			List<KeyValue> familyDeletes = deleteEntry.getValue();
			for (KeyValue keyDeletes : familyDeletes) {
				byte[] deleteQualifier = keyDeletes.getQualifier();
				byte[] kvQualifier = kv.getQualifier();
				if (keyDeletes.getTimestamp() > kv.getTimestamp() && (keyDeletes.isDeleteFamily() || Bytes.equals(deleteQualifier, kvQualifier))) {
					return null;
				}
			}
		}
		return kv;
	}

	public static void createIndexTableAndNodes(IndexTableStructure its, String schemaPath, String tableName, HTableDescriptor desc, RecoverableZooKeeper rzk, ObserverContext<MasterCoprocessorEnvironment> ctx) {
		if (LOG.isDebugEnabled())
			LOG.debug("Create index table and nodes for table: " + tableName);
		HTableDescriptor idesc = new HTableDescriptor(SchemaUtils.appendSuffix(tableName, INDEX));
		createIndexTableNode(schemaPath, tableName, rzk);
		//add default family
		idesc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
				HBaseConstants.DEFAULT_VERSIONS,
				HBaseConstants.DEFAULT_COMPRESSION,
				HBaseConstants.DEFAULT_IN_MEMORY,
				HBaseConstants.DEFAULT_BLOCKCACHE,
				HBaseConstants.DEFAULT_TTL,
				HBaseConstants.DEFAULT_BLOOMFILTER));
		if (its != null) {
			//Create index node and ensure column families for all columns
			if (its.hasIndexes()) {
				//Create index table node
				for (Index index : its.getIndexes()) {
					createIndexNode(schemaPath, tableName, index.getIndexName(), Bytes.toBytes(index.toJSon()), rzk);
					if (LOG.isDebugEnabled())
						LOG.debug("create index node in zoo " + index.getIndexName() + " json " + index.toJSon());
					for (String family : index.getAllFamilies()) {
						if (family.equals(HBaseConstants.DEFAULT_FAMILY)) continue;
						idesc.addFamily(new HColumnDescriptor(family));
					}
				}
			}
		}
		//add base row key family
		idesc.addFamily(new HColumnDescriptor(INDEX_BASE_FAMILY_BYTE));
		try {
			ctx.getEnvironment().getMasterServices().createTable(idesc, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void changeIndex(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, ObserverContext<RegionCoprocessorEnvironment> e) {
		try {
			if (Bytes.equals(put.getRow(), SchemaConstants.ADD_INDEX_BYTE)) {
				addIndex(schemaPath, tableNameStr, put, rzk, e.getEnvironment().getConfiguration());
				e.bypass();
				e.complete();
			} else if (Bytes.equals(put.getRow(), SchemaConstants.DELETE_INDEX_BYTE)) {
				deleteIndex(schemaPath, tableNameStr, put, rzk, e.getEnvironment().getConfiguration());
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

	public static void addIndex(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, Configuration conf) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Add index to table " + tableNameStr);
		String indexStr = Bytes.toString(put.getAttribute(SchemaConstants.SERIALIZED_INDEX));
		if (indexStr == null)
			throw new RuntimeException("Try to add a new index without serialized index definition.");
		Index index = Index.toIndex(indexStr);
		if (hasIndex(schemaPath, tableNameStr, index.getIndexName(), rzk))
			throw new RuntimeException("Try to add an existing index.");
		String indexPath = rzk.create(SchemaUtils.getIndexPath(schemaPath, tableNameStr, index.getIndexName()), Bytes.toBytes(index.toJSon()), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		if (LOG.isDebugEnabled())
			LOG.debug("Created an index in " + indexPath);
		addFamilyToTable(tableNameStr, index, conf);
		DictionaryMapReduceUtil.addIndex(Bytes.toBytes(tableNameStr), index, HConstants.LATEST_TIMESTAMP, conf);
	}

	public static void deleteIndex(String schemaPath, String tableNameStr, Put put, RecoverableZooKeeper rzk, Configuration conf) throws InterruptedException, KeeperException, IOException, ClassNotFoundException {
		if (LOG.isDebugEnabled())
			LOG.debug("Delete index from table " + tableNameStr);
		//using indexName only for deleting index
		String indexName = Bytes.toString(put.getAttribute(SchemaConstants.SERIALIZED_INDEX));
		if (indexName == null)
			throw new RuntimeException("Try to delete a index without serialized index name.");
		if (!hasIndex(schemaPath, tableNameStr, indexName, rzk))
			throw new RuntimeException("Try to delete an unexisting index.");
		rzk.delete(SchemaUtils.getIndexPath(schemaPath, tableNameStr, indexName), -1);
		DictionaryMapReduceUtil.deleteIndex(Bytes.toBytes(SchemaUtils.appendSuffix(tableNameStr, INDEX)), indexName, HConstants.LATEST_TIMESTAMP, conf);
	}

	public static void addFamilyToTable(String tableName, Index index, Configuration conf) {
		String indexTableName = SchemaUtils.appendSuffix(tableName, INDEX);
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(indexTableName));
			admin.disableTable(indexTableName);
			for (String family : index.getAllFamilies()) {
				if (desc.hasFamily(Bytes.toBytes(family))) continue;
				if (LOG.isDebugEnabled())
					LOG.debug("add new family " + family + " to table " + indexTableName);
				admin.addColumn(indexTableName, new HColumnDescriptor(family));
			}
			admin.enableTable(indexTableName);
		} catch (IOException e) {
			LOG.debug("Failed to add new family to table " + indexTableName);
			e.printStackTrace();
		}
	}

	public static boolean isIndexChngeEnv(Put put) {
		return Bytes.equals(put.getRow(), SchemaConstants.ADD_INDEX_BYTE) || Bytes.equals(put.getRow(), SchemaConstants.DELETE_INDEX_BYTE);
	} 

	public static void deleteIdxTablePathRecursively(String schemaPath, String tableName, ZooKeeperWatcher zkw) {
		RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
		try {
			if (rzk.getChildren(SchemaUtils.getIndexTablePath(schemaPath, tableName), false) != null)
				ZKUtil.deleteNodeRecursively(zkw, SchemaUtils.getIndexTablePath(schemaPath, tableName));
			else
				rzk.delete(SchemaUtils.getIndexTablePath(schemaPath, tableName), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static byte[] toSortable(byte[] data, Column.Type type, boolean includeEof) {
		switch (type) {
		case STRING:
			if (includeEof)
				return Bytes.add(data, EOF_MARKER);
			else
				return data;
		case BOOLEAN:
		case BYTE:
			return data;
		case BIGINT:
			return SortableByteUtil.toBytes(Bytes.toLong(data));
		case DATE:
			return SortableByteUtil.toBytes(Bytes.toLong(data));
		case CALENDAR:
			return SortableByteUtil.toBytes(Bytes.toLong(data));
		case LONG:
			return SortableByteUtil.toBytes(Bytes.toLong(data));
		case INTEGER:
			return SortableByteUtil.toBytes(Bytes.toInt(data));
		case FLOAT:
			return SortableByteUtil.toBytes(Bytes.toFloat(data));
		case DOUBLE:
			return SortableByteUtil.toBytes(Bytes.toDouble(data));
		case NOT_SUPPORTED:
		default:
			throw new RuntimeException("Column type not supported.");
		}
	}

	public static byte[] toBytesFromObject(Object object, Column.Type type) {
		switch (type) {
		case STRING:
			return Bytes.toBytes((String) object);
		case BIGINT:
			return Bytes.toBytes((Long) object);
		case BOOLEAN:
			return Bytes.toBytes((Boolean) object);
		case BYTE:
			return Bytes.toBytes((Byte) object);
		case DATE:
			return Bytes.toBytes(((Date) object).getTime());
		case CALENDAR:
			return Bytes.toBytes(((Calendar) object).getTime().getTime());
		case LONG:
			return Bytes.toBytes((Long) object);
		case INTEGER:
			return Bytes.toBytes((Integer) object);
		case FLOAT:
			return Bytes.toBytes((Float) object);
		case DOUBLE:
			return Bytes.toBytes((Double) object);
		case NOT_SUPPORTED:
		default:
			throw new RuntimeException("Column type not supported.");
		}
	}
	
	public static byte[] toSortableBytesFromObject(Object object, Column.Type type, boolean includeEof) {
		switch (type) {
		case STRING:
			if (includeEof) {
				return Bytes.add(Bytes.toBytes((String) object), EOF_MARKER);
			} else {
				return Bytes.toBytes((String) object);
			}
		case BIGINT:
			return SortableByteUtil.toBytes((Long) object);
		case BOOLEAN:
			return Bytes.toBytes((Boolean) object);
		case BYTE:
			return Bytes.toBytes((Byte) object);
		case DATE:
			return SortableByteUtil.toBytes(((Date) object).getTime());
		case CALENDAR:
			return SortableByteUtil.toBytes(((Calendar) object).getTime().getTime());
		case LONG:
			return SortableByteUtil.toBytes((Long) object);
		case INTEGER:
			return SortableByteUtil.toBytes((Integer) object);
		case FLOAT:
			try {
				return SortableByteUtil.toBytes((Float) object);
			} catch (Exception e) {
				return SortableByteUtil.toBytes(new Float((Double)object));
			}
			case DOUBLE:
			return SortableByteUtil.toBytes((Double) object);
		case NOT_SUPPORTED:
		default:
			throw new RuntimeException("Column type not supported.");
		}
	}
}
