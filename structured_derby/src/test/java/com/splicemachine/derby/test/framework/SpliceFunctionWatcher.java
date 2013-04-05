package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceFunctionWatcher extends TestWatcher {
	public static final String CREATE_FUNCTION = "create function ";
	private static final Logger LOG = Logger.getLogger(SpliceFunctionWatcher.class);
	protected String functionName;
	protected String schemaName;
	protected String createString;
	public SpliceFunctionWatcher(String functionName,String schemaName, String createString) {
		this.functionName = functionName.toUpperCase();
		this.schemaName = schemaName.toUpperCase();
		this.createString = createString;		
	}
	@Override
	protected void starting(Description description) {
		LOG.trace("Starting");
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		try {
			connection = SpliceNetConnection.getConnection();
			rs = connection.getMetaData().getTables(null, schemaName, functionName, null);
			if (rs.next()) {
				executeDrop(schemaName,functionName);
			}
			connection.commit();
			statement = connection.createStatement();
			statement.execute(CREATE_FUNCTION + schemaName + "." + functionName + " " + createString);
			connection.commit();
		} catch (Exception e) {
			LOG.error("Create function statement is invalid ");
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
		executeDrop(schemaName,functionName);
	}
	
	public static void executeDrop(String schemaName,String functionName) {
		LOG.trace("executeDrop");
		Connection connection = null;
		Statement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.createStatement();
			statement.execute("drop function " + schemaName.toUpperCase() + "." + functionName.toUpperCase());
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
