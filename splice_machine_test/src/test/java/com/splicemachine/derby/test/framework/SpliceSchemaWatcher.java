package com.splicemachine.derby.test.framework;

import com.splicemachine.test_dao.SchemaDAO;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

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
            SchemaDAO schemaDAO = new SchemaDAO(connection);
            rs = connection.getMetaData().getSchemas(null, schemaName);
            if (rs.next()) {
                schemaDAO.drop(schemaName);
            }
            connection.commit();
            statement = connection.createStatement();
            if (userName != null)
                statement.execute(String.format("create schema %s AUTHORIZATION %S", schemaName, userName));
            else
                statement.execute(String.format("create schema %s", schemaName));
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
     *
     * @param message message to be potentially tagged
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
