package com.splicemachine.test_dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

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
            //TODO -sf- this breaks something
//            try(ResultSet resultSet = metaData.getProcedures(null,uSchema,null)){
//                while(resultSet.next()){
//                    jdbcTemplate.executeUpdate(String.format("drop procedure %s.%s",uSchema,resultSet.getString("PROCEDURE_NAME")));
//                }
//            }

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
