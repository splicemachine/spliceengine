package com.ir.hbase.coprocessor;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import com.ir.constants.SchemaConstants;

public class SchemaUtils extends SchemaConstants {
	private static Logger LOG = Logger.getLogger(SchemaUtils.class);
	/**
	 * Creates the specified node and all parent nodes required for it to exist.
	 *
	 * No watches are set and no errors are thrown if the node already exists.
	 *
	 * The nodes created are persistent and open access.
	 */
	public static void createWithParents(RecoverableZooKeeper rzk, String znode) throws KeeperException {
		if (LOG.isTraceEnabled())
			LOG.trace("Create Node with Parents called for " + znode);		
		try {
			if(znode == null) {
				return;
			}
			rzk.create(znode, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch(KeeperException.NodeExistsException nee) {
			return;
		} catch(KeeperException.NoNodeException nne) {
			createWithParents(rzk, getParent(znode));
			createWithParents(rzk, znode);
		} catch(InterruptedException ie) {
			ie.printStackTrace();
		}
	}
	public static void createWithParent(RecoverableZooKeeper rzk, String znode, byte[] data) throws KeeperException {
		createWithParents(rzk, znode);
		try {
			rzk.setData(znode, data, -1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Returns the full path of the immediate parent of the specified node.
	 * @param node path to get parent of
	 * @return parent of path, null if passed the root node or an invalid node
	 */
	public static String getParent(String node) {
		int idx = node.lastIndexOf(PATH_DELIMITER);
		return idx <= 0 ? null : node.substring(0, idx);
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
	
	public static String appendSuffix(String tableName, String suffix) {
		StringBuilder sbuilder = new StringBuilder(tableName);
		sbuilder.append(suffix);
		return sbuilder.toString();
	}
	
	public static String getTablePath(String schemaPath, String tableName) {
		StringBuilder sbuilder = new StringBuilder(schemaPath);
		sbuilder.append(PATH_DELIMITER);
		sbuilder.append(tableName);
		return sbuilder.toString();
	}
	
	public static String getFamilyPath(String schemaPath, String tableName, String family) {
		StringBuilder sbuilder = new StringBuilder(getTablePath(schemaPath, tableName));
		sbuilder.append(PATH_DELIMITER);
		sbuilder.append(family);
		return sbuilder.toString();
	}
	
	public static String getColumnPath(String schemaPath, String tableName, String family, String columnName) {
		StringBuilder sbuilder = new StringBuilder(getFamilyPath(schemaPath, tableName, family));
		sbuilder.append(PATH_DELIMITER);
		sbuilder.append(columnName);
		return sbuilder.toString();
	}
	
	public static String getIndexTablePath(String schemaPath, String tableName) {
		return appendSuffix(getTablePath(schemaPath, tableName), INDEX);
	}
	
	public static String getIndexPath(String schemaPath, String tableName, String indexName) {
		StringBuilder sbuilder = new StringBuilder(getIndexTablePath(schemaPath, tableName));
		sbuilder.append(PATH_DELIMITER);
		sbuilder.append(indexName);
		return sbuilder.toString();
	}
	
	public static String appendSubPath(String path, String subPath) {
		StringBuilder sbuilder = new StringBuilder(path);
		sbuilder.append(PATH_DELIMITER);
		sbuilder.append(subPath);
		return sbuilder.toString();
	}
}
