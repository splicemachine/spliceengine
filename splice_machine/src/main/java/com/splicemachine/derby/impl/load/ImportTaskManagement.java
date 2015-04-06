package com.splicemachine.derby.impl.load;

import java.util.Map;

import javax.management.MXBean;

/**
 * Interface for wrapper class for JMX management statistics of Import Tasks.
 *
 * @author David Winters
 * Date: 4/1/2015
 */
@MXBean
public interface ImportTaskManagement {

	/**
	 * Return a map of the current total number of rows that have been imported, keyed by the import file path.
	 *
	 * @return the total number of imported rows
	 */
	Map<String, Long> getTotalImportedRowsByFilePath();

	/**
	 * Return a map of the current total number of rows that have been rejected as "bad", keyed by the import file path.
	 *
	 * @return the total number of rejected "bad" rows
	 */
	Map<String, Long> getTotalBadRowsByFilePath();
}
