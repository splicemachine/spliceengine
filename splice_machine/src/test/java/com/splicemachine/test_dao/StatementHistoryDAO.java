package com.splicemachine.test_dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/*
 * Provides access to SYS.SYSSTATEMENTHISTORY
 */
public class StatementHistoryDAO {

    private JDBCTemplate jdbcTemplate;

    private static final StatementHistoryRowMapper ROW_MAPPER = new StatementHistoryRowMapper();

    public StatementHistoryDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    /**
     * Returns rows from the statement history where the statement SQL contains the specified searchText.
     */
    public StatementHistory findStatement(String searchText) throws SQLException {
        String sql = String.format("select * from SYS.SYSSTATEMENTHISTORY where LOCATE('%s', STATEMENTSQL) > 0", searchText);
        return jdbcTemplate.queryForObject(sql, ROW_MAPPER);
    }

    /**
     * Returns rows from the statement history where the statement SQL contains the specified searchText.
     */
    public StatementHistory findStatement(String searchText, long waitTime, TimeUnit waitUnit) throws SQLException {
        String sql = String.format("select * from SYS.SYSSTATEMENTHISTORY where LOCATE('%s', STATEMENTSQL) > 0", searchText);
        return jdbcTemplate.queryForObjectWithWait(waitTime, waitUnit, sql, ROW_MAPPER);
    }


}
