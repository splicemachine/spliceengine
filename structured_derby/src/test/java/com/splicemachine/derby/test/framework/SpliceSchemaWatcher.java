package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceSchemaWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceSchemaWatcher.class);
	public String schemaName;
	protected String userName;
	public SpliceSchemaWatcher(String schemaName) {
		this.schemaName = schemaName.toUpperCase();
	}
	
	public SpliceSchemaWatcher(String schemaName, String userName) {
		this(schemaName);
		this.userName = userName;
	}
	
	@Override
	protected void starting(Description description) {
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			connection = SpliceNetConnection.getConnection();
			rs = connection.getMetaData().getSchemas(null, schemaName);
			if (rs.next())
				executeDrop(schemaName);
			connection.commit();
			statement = connection.createStatement();
			if (userName != null)
				statement.execute(String.format("create schema %s AUTHORIZATION %S",schemaName,userName));
			else 
				statement.execute(String.format("create schema %s",schemaName));
		} catch (Exception e) {
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
		LOG.trace("Finished");
	}
	
	public static void executeDrop(String schemaName) {
		LOG.trace("ExecuteDrop");
		Connection connection = null;
		Statement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			ResultSet resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, new String[]{"VIEW"});
			while (resultSet.next()) {
				SpliceTableWatcher.executeDrop(schemaName, resultSet.getString("TABLE_NAME"), true);
			}
            resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, null);
			while (resultSet.next()) {
				SpliceTableWatcher.executeDrop(schemaName, resultSet.getString("TABLE_NAME"));
			}
			
			statement = connection.createStatement();
			resultSet = connection.getMetaData().getSchemas(null, schemaName.toUpperCase());
			while (resultSet.next()) {
				statement.execute("drop schema " + schemaName + " RESTRICT");
			}
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
