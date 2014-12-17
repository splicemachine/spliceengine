package com.splicemachine.derby.test.framework;

import java.sql.*;

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
				executeDrop(schemaName, connection);
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
		LOG.trace(tag("Finished", schemaName));
	}

	/**
	 * Drop the given schema after dropping all dependent objects. Creates a connection.
	 * @param schemaName the schema to drop
	 */
	public static void executeDrop(String schemaName) {
		Connection connection = null;
		try {
			connection = SpliceNetConnection.getConnection();
			executeDrop(schemaName, connection);
			connection.commit();
		} catch (Exception e) {
			LOG.error(tag("error Dropping " + e.getMessage(), schemaName));
			e.printStackTrace();
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException e1) {
                LOG.error(tag("error Rolling back " + e1.getMessage(), schemaName));
                e1.printStackTrace();
            }
            throw new RuntimeException(e);
		} finally {
			DbUtils.commitAndCloseQuietly(connection);
		}
	}

	/**
	 * Drop the given schema after dropping all dependent objects.
	 * @param schemaName the schema to drop
	 * @param connection the open connection to use. Allow passing in a Connection
	 *                   so that these drops can participate in the same transaction.
	 * @see com.splicemachine.derby.test.framework.SpliceSchemaWatcher#executeDrop(String)
	 */
	public static void executeDrop(String schemaName, Connection connection) {
		LOG.trace(tag("ExecuteDrop", schemaName));
		assert connection != null;
		Statement statement = null;
		try {
			ResultSet resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, new String[]{"VIEW"});
			while (resultSet.next()) {
				SpliceTableWatcher.executeDrop(schemaName, resultSet.getString("TABLE_NAME"), true, connection);
			}
			resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, null);
			while (resultSet.next()) {
				SpliceTableWatcher.executeDrop(schemaName, resultSet.getString("TABLE_NAME"), false, connection);
			}
			resultSet = connection.getMetaData().getProcedures(null, schemaName.toUpperCase(), null);
			statement = connection.createStatement();
			while (resultSet.next()) {
				statement.execute(String.format("drop procedure %s.%s",schemaName.toUpperCase(),resultSet.getString("PROCEDURE_NAME")));
			}

			resultSet = connection.getMetaData().getSchemas(null, schemaName.toUpperCase());
			while (resultSet.next()) {
				statement.execute("drop schema " + schemaName + " RESTRICT");
			}
		} catch (Exception e) {
			LOG.error(tag("error Dropping " + e.getMessage(), schemaName));
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(statement);
		}

	}

    @Override
    public String toString() {
        return schemaName;
    }

    //-----------------------------------------------------------------------------------------
    // The following methods are for tagging the log messages with additional information
    // related to the schema and table.
    //-----------------------------------------------------------------------------------------

    /**
     * Tag the message with extra information (schema name) if the message is a String.
     * @param message  message to be potentially tagged
     * @param schema  name of schema
     */
    protected static Object tag(Object message, String schema) {
    	if (message instanceof String) {
    		return String.format("[%s] %s", schema, message);
    	} else {
    		return message;
    	}
    }
}
