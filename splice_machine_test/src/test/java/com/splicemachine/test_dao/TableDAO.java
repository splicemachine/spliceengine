package com.splicemachine.test_dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;

import static java.lang.String.format;

/**
 * Manipulate tables.
 */
public class TableDAO {

    private JDBCTemplate jdbcTemplate;

    public TableDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    /**
     * Drop the specified tables.
     */
    public void drop(String schemaName, String... tableNames) {
        for (String tableName : tableNames) {
            drop(schemaName, tableName, false);
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
                    jdbcTemplate.executeUpdate(format("drop view %s.%s", schemaName, tableName));
                } else {
                    jdbcTemplate.executeUpdate(format("drop table if exists %s.%s", schemaName, tableName));
                }
            }
        } catch (Exception e) {
            // Drop failed because of FK constraint.
            if (e.getMessage().contains("DROP CONSTRAINT") && e.getMessage().contains("is dependent on that object")) {
                dropTableRecursive(schemaName, tableName);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Delete the specified table, deleting any tables with FKs referencing it first, recursively.
     */
    public void dropTableRecursive(String schemaName, String tableName) {
        try {
            List<String> dependentTableNames = getDependentTableNames(schemaName, tableName);
            for (String dependentTableName : dependentTableNames) {
                dropTableRecursive(schemaName, dependentTableName);
            }
            jdbcTemplate.executeUpdate(format("drop table if exists %s.%s", schemaName, tableName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a list of table names in the specified schema that have FKs referencing the specified table.
     */
    private List<String> getDependentTableNames(String schemaName, String tableName) {
        String sql = "" +
                "select t.tablename " +
                " from sys.systables t " +
                "  join sys.SYSCONSTRAINTS c on c.tableid = t.tableid " +
                "  where c.constraintid in ( " +
                "    select fk.CONSTRAINTID " +
                "    from sys.sysforeignkeys fk " +
                "      join sys.sysconstraints c on fk.keyconstraintid = c.constraintid " +
                "      join sys.systables      t on t.tableid  = c.tableid " +
                "      join sys.sysschemas     s on s.schemaid = t.schemaid " +
                "    where s.SCHEMANAME=? " +
                "          and t.tablename=?      )";
        return jdbcTemplate.query(sql, schemaName, tableName);
    }

}
