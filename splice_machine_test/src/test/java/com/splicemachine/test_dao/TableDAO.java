package com.splicemachine.test_dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

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

    /**
     * Drop the given table or view.
     */
    public void drop(String schemaName, String tableName, boolean isView) {
        try {
            DatabaseMetaData metaData = jdbcTemplate.getConnection().getMetaData();
            ResultSet rs = metaData.getTables(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
            if (rs.next()) {
                if (isView) {
                    jdbcTemplate.executeUpdate(String.format("drop view %s.%s", schemaName.toUpperCase(), tableName.toUpperCase()));
                } else {
                    jdbcTemplate.executeUpdate(String.format("drop table if exists %s.%s", schemaName.toUpperCase(), tableName.toUpperCase()));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void drop(String schemaName, String tableName) {
        drop(schemaName, tableName, false);
    }

}
