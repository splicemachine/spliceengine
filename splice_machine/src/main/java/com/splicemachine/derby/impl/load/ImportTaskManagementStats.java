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
import org.apache.log4j.Logger;

/**
 * Wrapper for JMX management statistics for import tasks.
 * This class is a Singleton since the underlying map is what is needed from JMX
 * and not the actual object other than a couple of convenience methods
 * to return the maps.
 * 
 * @author dwinters
 */
public class ImportTaskManagementStats implements ImportTaskManagement {

  private static final Logger LOG = Logger.getLogger(ImportTaskManagementStats.class);
	/**
	 * Map of the current total number of rows that have been imported, keyed by the import task path.
	 * Used by the ImportTaskManagement JMX MBean.
	 */
	private static final ConcurrentHashMap<String, Long> importedRowsMap = new ConcurrentHashMap<>();

	/**
	 * Map of the current total number of rows that have been rejected as "bad", keyed by the import task path.
	 * Used by the ImportTaskManagement JMX MBean.
	 */
	private static final ConcurrentHashMap<String, Long> badRowsMap = new ConcurrentHashMap<>();

	/**
	 * Map of the current file paths that are being imported, keyed by the import task path.
	 * Used by the ImportTaskManagement JMX MBean.
	 */
	private static final ConcurrentHashMap<String, String> filePathsMap = new ConcurrentHashMap<>();

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
	 * Singleton factory method.
	 */
	public static ImportTaskManagementStats getInstance() {
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
	public void registerJMX(MBeanServer mbs)
			throws MalformedObjectNameException,
			NotCompliantMBeanException,
			InstanceAlreadyExistsException,
			MBeanRegistrationException {
		mbs.registerMBean(stats, new ObjectName(JMXUtils.IMPORT_TASK_MANAGEMENT));
	}

	/**
	 * Initialize the map for this import task path.
	 *
	 * @param importTaskPath
	 * @param importFilePath
	 */
	public void initialize(String importTaskPath, String importFilePath) {
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Initializing: taskPath=%s, filePath=%s", importTaskPath, importFilePath));
		if (importTaskPath == null) return;
		importedRowsMap.put(importTaskPath, 0l);
		badRowsMap.put(importTaskPath, 0l);
		filePathsMap.put(importTaskPath, importFilePath);
	}

	/**
	 * Clean up the map for this import task path.
	 *
	 * @param importTaskPath
	 */
	public void cleanup(String importTaskPath) {
		if (importTaskPath == null) return;  // This can happen for RollForward tasks that are not distributed, thus they do not have nodes in ZooKeeper.
		Object removed = importedRowsMap.remove(importTaskPath);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("cleanup(): taskPath=%s, importedRowsMap entry removed=%s", importTaskPath, removed));

		badRowsMap.remove(importTaskPath);
        filePathsMap.remove(importTaskPath);
	}

	/**
	 * Set the imported row count for this import task path.
	 * 
	 * @param importTaskPath
	 * @param rowCount
	 */
	public void setImportedRowCount(String importTaskPath, long rowCount) {
		importedRowsMap.put(importTaskPath, rowCount);
	}

	/**
	 * Set the "bad" row count for this import task path.
	 *
	 * @param importTaskPath
	 * @param rowCount
	 */
	public void setBadRowCount(String importTaskPath, long rowCount) {
		badRowsMap.put(importTaskPath, rowCount);
	}

	@Override
	public Map<String, Long> getTotalImportedRowsByTaskPath() {
		return importedRowsMap;
	}

	@Override
	public Map<String, Long> getTotalBadRowsByTaskPath() {
		return badRowsMap;
	}

	@Override
	public Map<String, String> getImportFilePathsByTaskPath() {
		return filePathsMap;
	}
}
