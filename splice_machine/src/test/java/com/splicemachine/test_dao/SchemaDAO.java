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
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    /**
     * Drop the given schema after dropping all dependent objects.
     */
    public void drop(String schemaName) {
        Connection connection = jdbcTemplate.getConnection();
        try {
            DatabaseMetaData metaData=connection.getMetaData();
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

            // Delete Triggers

            try(PreparedStatement ps=jdbcTemplate.getConnection().prepareStatement(String.format("select triggername from sys.systriggers, sys.sysschemas where "+
                    "sys.systriggers.schemaid = sys.sysschemas.schemaid and sys.sysschemas.schemaname = '%s'",uSchema))){
                try(ResultSet resultSet=ps.executeQuery()){
                    while(resultSet.next()){
                        jdbcTemplate.executeUpdate(String.format("drop trigger %s.%s",uSchema,resultSet.getString(1)));
                    }
                }
            }catch(Exception e){
                throw new RuntimeException(e);
            }

            // Delete Alias
            // TODO JL

            // Delete Files

            try(PreparedStatement ps=jdbcTemplate.getConnection().prepareStatement(String.format("select FILENAME from sys.SYSFILES, sys.sysschemas where "+
                    "sys.SYSFILES.schemaid = sys.sysschemas.schemaid and sys.sysschemas.schemaname = '%s'",uSchema))){
                try(ResultSet resultSet = ps.executeQuery()){
                    while(resultSet.next()){
                        jdbcTemplate.executeUpdate(String.format("CALL SQLJ.REMOVE_JAR('%s', 0)",uSchema+"."+resultSet.getString(1)));
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


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
