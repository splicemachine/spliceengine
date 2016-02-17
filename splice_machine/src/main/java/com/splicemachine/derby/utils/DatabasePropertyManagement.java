package com.splicemachine.derby.utils;

import java.sql.SQLException;

import javax.management.MXBean;

/**
 * Interface for wrapper class for JMX management statistics of Database Properties.
 *
 * @author David Winters
 * Date: 4/23/2015
 */
@MXBean
public interface DatabasePropertyManagement {

	/**
	 * Get the value of a database property.
	 *
	 * @param key  key of the database property
	 *
	 * @return value of the database property
	 *
	 * @throws SQLException
	 */
	String getDatabaseProperty(String key) throws SQLException;

	/**
	 * Set the value of a database property.
	 *
	 * @param key  key of the database property
	 * @param value  value of the database property
	 *
	 * @throws SQLException
	 */
	void setDatabaseProperty(String key,String value) throws SQLException;
}
