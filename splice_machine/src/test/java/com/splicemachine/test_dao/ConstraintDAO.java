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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Query sys.sysconstraints.
 */
public class ConstraintDAO {

    private RowMapper<Constraint> CONSTRAINT_ROW_MAPPER = new ConstraintRowMapper();

    private JDBCTemplate jdbcTemplate;

    public ConstraintDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
    }

    public List<Constraint> getAllConstraints(String tableName) {
        final String sql = "" +
                "select sc.* " +
                "from sys.sysconstraints sc " +
                "join sys.systables      t  on t.tableid  = sc.tableid " +
                "join sys.sysschemas     s  on s.schemaid = sc.schemaid " +
                "where s.schemaname=CURRENT SCHEMA and t.tablename=?";
        return jdbcTemplate.query(sql, CONSTRAINT_ROW_MAPPER, tableName.toUpperCase());
    }

    private static class ConstraintRowMapper implements RowMapper<Constraint> {

        @Override
        public Constraint map(ResultSet resultSet) throws SQLException {
            Constraint constraint = new Constraint();
            constraint.setConstraintId(resultSet.getString(1));
            constraint.setTableId(resultSet.getString(2));
            constraint.setConstraintName(resultSet.getString(3));
            constraint.setType(resultSet.getString(4));
            constraint.setSchemaId(resultSet.getString(5));
            constraint.setState(resultSet.getString(6));
            constraint.setReferenceCount(resultSet.getInt(7));
            return constraint;
        }
    }

}
