package com.splicemachine.test_dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/*
 * Provides access to SYS.SYSTASKHISTORY
 */
public class TaskHistoryDAO {

    private static final TaskHistoryRowMapper ROW_MAPPER = new TaskHistoryRowMapper();

    private JDBCTemplate jdbcTemplate;

    public TaskHistoryDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    /**
     * Get task history entries by statement ID.
     */
    public List<TaskHistory> getByStatementId(long statementId) throws SQLException {
        return jdbcTemplate.query("select * from sys.SYSTASKHISTORY where STATEMENTID=?", ROW_MAPPER, statementId);
    }

    /**
     * Get task history entries by statement ID, waiting for up to specified time.
     */
    public List<TaskHistory> getByStatementId(long statementId, long waitTime, TimeUnit waitUnit) throws SQLException {
        return jdbcTemplate.queryWithWait(waitTime, waitUnit, "select * from sys.SYSTASKHISTORY where STATEMENTID=?", ROW_MAPPER, statementId);
    }
}
