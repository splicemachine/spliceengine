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

package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceRoleWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceRoleWatcher.class);
	protected String roleName;
	public SpliceRoleWatcher(String roleName) {
		this.roleName = roleName;		
	}

	
	@Override
	protected void starting(Description description) {
		LOG.trace("Starting");
		executeDrop(roleName.toUpperCase());
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.createStatement();
			statement.execute(String.format("create role %s",roleName));
			connection.commit();
		} catch (Exception e) {
			LOG.error("Role statement is invalid ");
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
		super.starting(description);
	}
	@Override
	protected void finished(Description description) {
		LOG.trace("finished");
	}
	
	public static void executeDrop(String roleName) {
		LOG.trace("ExecuteDrop");
		Connection connection = null;
		PreparedStatement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.prepareStatement("select roleid from sys.sysroles where roleid = ?");
			statement.setString(1, roleName);
			ResultSet rs = statement.executeQuery();
			if (rs.next())
				connection.createStatement().execute(String.format("drop role %s",roleName));
			connection.commit();
		} catch (Exception e) {
			LOG.error("error Dropping " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
	}
	
}
