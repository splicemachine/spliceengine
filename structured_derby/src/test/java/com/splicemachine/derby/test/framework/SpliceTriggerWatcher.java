package com.splicemachine.derby.test.framework;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Jeff Cunningham
 *         Date: 6/5/13
 */
public class SpliceTriggerWatcher extends TestWatcher {
    public static final String CREATE_TRIGGER = "create trigger ";
    private static final Logger LOG = Logger.getLogger(SpliceFunctionWatcher.class);
    protected String triggerName;
    protected String schemaName;
    protected String createString;
    // TODO: add more specific trigger building params
/*
CREATE TRIGGER TriggerName
AFTER
{ INSERT | DELETE | UPDATE [ OF column-Name [, column-Name]* ]
ON table-Name
[ ReferencingClause ]
FOR EACH { ROW | STATEMENT } MODE DB2SQL
Triggered-SQL-statement

ReferencingClause:

REFERENCING
{
{ OLD | NEW } [ AS ] correlation-Name [ { OLD | NEW } [ AS ] correlation-Name ] |
{ OLD_TABLE | NEW_TABLE } [ AS ] Identifier [ { OLD_TABLE | NEW_TABLE }
[AS] Identifier ]
}
*/

    public SpliceTriggerWatcher(String trigger,String schemaName, String createString) {
        this.triggerName = trigger.toUpperCase();
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
            rs = connection.getMetaData().getTables(null, schemaName, triggerName, null);
            if (rs.next()) {
                executeDrop(schemaName, triggerName);
            }
            connection.commit();
            statement = connection.createStatement();
            statement.execute(CREATE_TRIGGER + schemaName + "." + triggerName + " " + createString);
            connection.commit();
        } catch (Exception e) {
            LOG.error("Create trigger statement is invalid ");
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
        executeDrop(schemaName, triggerName);
    }

    public static void executeDrop(String schemaName,String functionName) {
        LOG.trace("executeDrop");
        Connection connection = null;
        Statement statement = null;
        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.createStatement();
            statement.execute("drop trigger " + schemaName.toUpperCase() + "." + functionName.toUpperCase());
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
