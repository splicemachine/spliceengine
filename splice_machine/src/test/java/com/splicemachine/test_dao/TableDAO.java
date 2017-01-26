/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.test_dao;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.utils.EngineUtils;

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
            synchronized(jdbcTemplate){ //TODO -sf- synchronize on something else, or remove the need
                DatabaseMetaData metaData=jdbcTemplate.getConnection().getMetaData();
                String tN;
                if(tableName.toLowerCase().equals(tableName))
                    tN = tableName.toUpperCase();
                else if(tableName.toUpperCase().equals(tableName))
                    tN = tableName;
                else {
                    //we have mixed case
                    tN = "\""+tableName+"\"";
                }
                try(ResultSet rs=metaData.getTables(null,schemaName.toUpperCase(),tableName,null)){
                    if(rs.next()){
                        if(isView){
                            jdbcTemplate.executeUpdate(format("drop view %s.%s",schemaName,tN));
                        }else{
                            jdbcTemplate.executeUpdate(format("drop table %s.%s",schemaName,tN));
                        }
                    }
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
