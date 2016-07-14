/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
