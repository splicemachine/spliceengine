package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class SpliceTableWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceTableWatcher.class);
	protected String tableName;
	protected String schemaName;
	protected String createString;
	public SpliceTableWatcher(String tableName,String schemaName, String createString) {
		this.tableName = tableName.toUpperCase();
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
			rs = connection.getMetaData().getTables(null, schemaName, tableName, null);
			if (rs.next()) {
				executeDrop(schemaName,tableName);
			}
			connection.commit();
			statement = connection.createStatement();
			statement.execute(String.format("create table %s.%s %s",schemaName,tableName,createString));
			connection.commit();
		} catch (Exception e) {
			LOG.error("Create table statement is invalid ");
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
//		executeDrop(schemaName,tableName);
	}
	
	public static void executeDrop(String schemaName,String tableName) {
		LOG.trace("executeDrop");
		Connection connection = null;
		Statement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			ResultSet rs = connection.getMetaData().getTables(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
			if (rs.next()) {
				statement = connection.createStatement();
				statement.execute(String.format("drop table %s.%s",schemaName.toUpperCase(),tableName.toUpperCase()));
				connection.commit();
			}
		} catch (Exception e) {
			LOG.error("error Dropping " + e.getMessage(),e);
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(statement);
			DbUtils.commitAndCloseQuietly(connection);
		}
	}

	public void importData(String filename) {
		Connection connection = null;
		PreparedStatement ps = null;
		try {
			connection = SpliceNetConnection.getConnection();
		    ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (?, ?, null,null,?,',',null,null,null,null)");
		    ps.setString(1,schemaName);
		    ps.setString(2,tableName);  
		    ps.setString(3,filename);
		    ps.executeUpdate();
		} catch (Exception e) {
            LOG.error(e);
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(ps);
			DbUtils.commitAndCloseQuietly(connection);
		}
	}

    @Override
    public String toString() {
        return schemaName+"."+tableName;
    }
}
