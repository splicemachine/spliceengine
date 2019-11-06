/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Manipulate schemas.
 */
public class SchemaDAO {

    private final JDBCTemplate jdbcTemplate;
    private final TableDAO tableDAO;

    public SchemaDAO(Connection connection) {
        this.jdbcTemplate = new JDBCTemplate(connection);
        this.tableDAO = new TableDAO(connection);
    }

    public void cleanSchemaObjects(String schemaName,
                                   Connection connection,
                                   DatabaseMetaData metaData) throws SQLException {
        if (connection == null)
            connection = jdbcTemplate.getConnection();
        if (metaData == null)
            metaData=connection.getMetaData();

        String uSchema=schemaName.toUpperCase();
        //
        // Delete views
        //
        try(ResultSet resultSet = metaData.getTables(null,uSchema,null,new String[]{"VIEW"})){
            while(resultSet.next()){
                tableDAO.drop(schemaName,resultSet.getString("TABLE_NAME"),true);
            }
        }

        //
        // Deletes tables
        //
        try(ResultSet resultSet = metaData.getTables(null,uSchema,null,null)){
            while(resultSet.next()){
                tableDAO.drop(schemaName,resultSet.getString("TABLE_NAME"),false);
            }
        }

        //
        // Delete procedures
        //
        try(ResultSet resultSet = metaData.getProcedures(null,uSchema,null)){
            while(resultSet.next()){
                jdbcTemplate.executeUpdate(String.format("drop procedure %s.%s",uSchema,resultSet.getString("PROCEDURE_NAME")));
            }
        }

        //
        // Delete functions
        //
        try(ResultSet resultSet = metaData.getFunctions(null,uSchema,null)){
            while(resultSet.next()){
                jdbcTemplate.executeUpdate(String.format("drop function %s.%s",uSchema,resultSet.getString("FUNCTION_NAME")));
            }
        }

        // Delete Sequences

        try(ResultSet resultSet = jdbcTemplate.getConnection().prepareStatement(String.format("select sequencename from sys.syssequences seq, sys.sysschemas sch where " +
                "seq.schemaid = sch.schemaid and SCHEMANAME = '%s'",uSchema)).executeQuery()) {
            while(resultSet.next()){
                jdbcTemplate.executeUpdate(String.format("drop sequence %s.%s restrict",uSchema,resultSet.getString(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Delete Triggers

        try(ResultSet resultSet = jdbcTemplate.getConnection().prepareStatement(String.format("select triggername from sys.systriggers, sys.sysschemas where " +
                "sys.systriggers.schemaid = sys.sysschemas.schemaid and sys.sysschemas.schemaname = '%s'",uSchema)).executeQuery()) {
            while(resultSet.next()){
                jdbcTemplate.executeUpdate(String.format("drop trigger %s.%s",uSchema,resultSet.getString(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Delete Alias
        // TODO JL

        // Delete Files

        try(ResultSet resultSet = jdbcTemplate.getConnection().prepareStatement(String.format("select FILENAME from sys.SYSFILES, sys.sysschemas where " +
                "sys.SYSFILES.schemaid = sys.sysschemas.schemaid and sys.sysschemas.schemaname = '%s'",uSchema)).executeQuery()) {
            while(resultSet.next()){
                jdbcTemplate.executeUpdate(String.format("CALL SQLJ.REMOVE_JAR('%s', 0)",uSchema+"."+resultSet.getString(1)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Drop the given schema after dropping all dependent objects.
     */
    public void drop(String schemaName) {
        Connection connection = jdbcTemplate.getConnection();
        try {
            DatabaseMetaData metaData=connection.getMetaData();
            String uSchema=schemaName.toUpperCase();
            cleanSchemaObjects(schemaName, connection, metaData);

            //
            // Drop schema
            //
            try(ResultSet resultSet = metaData.getSchemas(null,uSchema)){
                while(resultSet.next()){
                    jdbcTemplate.executeUpdate("drop schema "+schemaName+" RESTRICT");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
