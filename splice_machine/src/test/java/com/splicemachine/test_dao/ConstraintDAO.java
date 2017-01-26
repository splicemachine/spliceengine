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
