package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
/**
 * TODO
 * @author johnleach
 *
 */
public class SpliceIndexWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceIndexWatcher.class);
	protected String tableName;
	protected String tableSchemaName;
	protected String createString;
	protected String indexName;
	protected String indexSchemaName;
	protected String create = "create";
	protected static String SELECT_SPECIFIC_INDEX = "select c.conglomeratename from sys.sysconglomerates c inner join sys.sysschemas s on " + 
									"c.schemaid = s.schemaid where c.isindex = 'TRUE' and s.schemaname = ? and c.conglomeratename = ?";
	
	
	public SpliceIndexWatcher(String tableName, String tableSchemaName, String indexName, String indexSchemaName, String createString) {
		this(tableName,tableSchemaName, indexName, indexSchemaName, createString, false);
	}

	public SpliceIndexWatcher(String tableName, String tableSchemaName, String indexName, String indexSchemaName, String createString, boolean unique) {
		this.tableName = tableName.toUpperCase();
		this.tableSchemaName = tableSchemaName.toUpperCase();
		this.createString = createString;
		this.indexName = indexName.toUpperCase();
		this.indexSchemaName = indexSchemaName.toUpperCase();
		if (unique)
			create = create+" unique";
	}

	@Override
	public void starting(Description description) {
		LOG.trace("Starting");
		Connection connection = null;
		PreparedStatement statement = null;
		Statement statement2 = null;
		ResultSet rs = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.prepareStatement(SELECT_SPECIFIC_INDEX);
			statement.setString(1, indexSchemaName);
			statement.setString(2, indexName);			
			rs = statement.executeQuery();
			if (rs.next()) {
				executeDrop(indexSchemaName,indexName);
			}
			connection.commit();
			statement2 = connection.createStatement();
			statement2.execute(String.format("%s index %s.%s on %s.%s %s",create,indexSchemaName,indexName,tableSchemaName,tableName,createString));
			connection.commit();
		} catch (Exception e) {
			LOG.error("Create index statement is invalid ");
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(statement);
			DbUtils.closeQuietly(statement2);
			DbUtils.commitAndCloseQuietly(connection);
		}
		super.starting(description);
	}
	@Override
	public void finished(Description description) {
		LOG.trace("finished");
		executeDrop(indexSchemaName,indexName);
	}
	
	public static void executeDrop(String indexSchemaName,String indexName) {
		LOG.trace("executeDrop");
		Connection connection = null;
		Statement statement = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.createStatement();
			statement.execute(String.format("drop index %s.%s",indexSchemaName.toUpperCase(),indexName.toUpperCase()));
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
