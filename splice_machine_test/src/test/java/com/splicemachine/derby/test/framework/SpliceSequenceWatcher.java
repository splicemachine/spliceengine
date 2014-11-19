package com.splicemachine.derby.test.framework;

import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by jyuan on 11/13/14.
 */
public class SpliceSequenceWatcher extends TestWatcher {
    private static final Logger LOG = Logger.getLogger(SpliceSequenceWatcher.class);
    private String schemaName;
    private String sequenceName;
    private String createString;

    public SpliceSequenceWatcher(String sequenceName,String schemaName, String createString) {
        this.sequenceName = sequenceName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.createString = createString;
    }

    @Override
    public void starting(Description description) {
        start();
        super.starting(description);
    }

    @Override
    protected void finished(Description description) {
        LOG.trace("finished");
//		executeDrop(schemaName,tableName);
    }

    @Override
    public String toString() {
        return schemaName + "." + sequenceName;
    }

    private void start() {
        LOG.trace("executeDrop " + schemaName + "." + sequenceName);
        Connection connection = null;
        Statement statement = null;

        executeDrop(schemaName, sequenceName);

        try{
            connection = SpliceNetConnection.getConnection();
            statement = connection.createStatement();
            statement.execute(String.format("create sequence %s.%s %s",schemaName,sequenceName,createString));
            connection.commit();
        } catch (Exception e) {
            error("Create sequence statement is invalid. Statement = "+ String.format("create sequence %s.%s %s",schemaName,sequenceName,createString),e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }

    }

    public void executeDrop(String schemaName,String sequenceName) {

        Connection connection = null;
        Statement statement = null;

        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(
                    String.format("select sch.schemaname, seq.sequencename from " +
                                          "sys.sysschemas sch, sys.syssequences seq " +
                                   "where sch.schemaid = seq.schemaid and seq.sequencename='%s' and schemaname='%s'"
                            ,sequenceName, schemaName)) ;
            if (rs.next()) {
                statement = connection.createStatement();
                statement.execute(String.format("drop sequence %s.%s restrict",schemaName.toUpperCase(),
                        sequenceName.toUpperCase()));
                connection.commit();
            }
        } catch (Exception e) {
            LOG.error(tag("error Dropping " + e.getMessage(), schemaName, sequenceName),e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    protected static Object tag(Object message, String schema, String table) {
        if (message instanceof String) {
            return String.format("[%s.%s] %s", schema, table, message);
        } else {
            return message;
        }
    }

    protected void error(Object message, Throwable t) {
        LOG.error(tag(message), t);
    }

    protected Object tag(Object message) {
        return tag(message, schemaName, sequenceName);
    }
}
