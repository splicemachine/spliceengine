package com.splicemachine.test_dao;

import java.sql.Connection;

/**
 * Manipulate tables.
 */
public class TableDAO {

    private JDBCTemplate jdbcTemplate;

    public TableDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    /**
     * Attempts to drop each table in the order passed, ignores any exceptions.
     */
    public void deleteTableForce(String... tableNames) {
        for (String tableName : tableNames) {
            try {
                jdbcTemplate.executeUpdate("drop table " + tableName);
            } catch (Exception e) {
                /* ignore per method contract */
            }
        }
    }

}
