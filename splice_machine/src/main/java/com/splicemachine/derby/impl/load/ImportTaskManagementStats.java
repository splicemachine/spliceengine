/**
 * 
 */
package com.splicemachine.derby.impl.load;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.splicemachine.hbase.jmx.JMXUtils;

/**
 * Wrapper for JMX management statistics for import tasks.
 * This class is a Singleton since the underlying map is what is needed from JMX
 * and not the actual object other than a couple of convenience methods
 * to return the maps.
 * 
 * @author dwinters
 */
public class ImportTaskManagementStats implements ImportTaskManagement {

	/**
	 * Map of the current total number of rows that have been imported, keyed by the import file path.
	 * Used by the ImportTaskManagement JMX MBean.
	 */
	private static final ConcurrentHashMap<String, Long> importedRowsMap = new ConcurrentHashMap<>();

	/**
	 * Map of the current total number of rows that have been rejected as "bad", keyed by the import file path.
	 * Used by the ImportTaskManagement JMX MBean.
	 */
	private static final ConcurrentHashMap<String, Long> badRowsMap = new ConcurrentHashMap<>();

	/**
	 * Singleton reference to self instance.
	 */
	private static ImportTaskManagementStats stats = new ImportTaskManagementStats();

	/**
	 *  Hide the constructor to force using the singleton factory method.
	 */
	private ImportTaskManagementStats() {
	}

	/**
	 *  Singleton factory method.
	 */
	public static ImportTaskManagementStats getImportTaskManagementStats() {
		return stats;
	}

	/**
	 * Register this implementation under JMX.
	 *
	 * @param mbs the MBeanServer to use
	 * @throws MalformedObjectNameException
	 * @throws NotCompliantMBeanException
	 * @throws InstanceAlreadyExistsException
	 * @throws MBeanRegistrationException
	 */
	public static void registerJMX(MBeanServer mbs)
			throws MalformedObjectNameException,
			NotCompliantMBeanException,
			InstanceAlreadyExistsException,
			MBeanRegistrationException {
		mbs.registerMBean(stats, new ObjectName(JMXUtils.IMPORT_TASK_MANAGEMENT));
	}

	/**
	 * Initialize the map for this import file path.
	 *
	 * @param importFilePath
	 */
	public static void initialize(String importFilePath) {
		importedRowsMap.put(importFilePath, 0l);
		badRowsMap.put(importFilePath, 0l);
	}

	/**
	 * Clean up the map for this import file path.
	 *
	 * @param importFilePath
	 */
	public static void cleanup(String importFilePath) {
		importedRowsMap.remove(importFilePath);
		badRowsMap.remove(importFilePath);
	}

	/**
	 * Set the imported row count for this import file path.
	 * 
	 * @param importFilePath
	 * @param rowCount
	 */
	public static void setImportedRowCount(String importFilePath, long rowCount) {
		importedRowsMap.put(importFilePath, rowCount);
	}

	/**
	 * Set the "bad" row count for this import file path.
	 *
	 * @param importFilePath
	 * @param rowCount
	 */
	public static void setBadRowCount(String importFilePath, long rowCount) {
		badRowsMap.put(importFilePath, rowCount);
	}

	/* (non-Javadoc)
	 * @see com.splicemachine.derby.impl.load.ImportTaskManagement#getTotalImportedRowsByFilePath()
	 */
	@Override
	public Map<String, Long> getTotalImportedRowsByFilePath() {
		return importedRowsMap;
	}

	/* (non-Javadoc)
	 * @see com.splicemachine.derby.impl.load.ImportTaskManagement#getTotalBadRowsByFilePath()
	 */
	@Override
	public Map<String, Long> getTotalBadRowsByFilePath() {
		return badRowsMap;
	}
}
