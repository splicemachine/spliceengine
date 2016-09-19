/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
