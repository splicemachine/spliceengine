package com.splicemachine.test_dao;

import java.sql.Connection;
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
     * Drop the given schema after dropping all dependent objects. Creates a connection.
     */
    public void drop(String schemaName) {
        Connection connection = jdbcTemplate.getConnection();
        try {
            //
            // Delete views
            //
            ResultSet resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, new String[]{"VIEW"});
            while (resultSet.next()) {
                tableDAO.drop(schemaName, resultSet.getString("TABLE_NAME"), true);
            }

            //
            // Deletes tables
            //
            resultSet = connection.getMetaData().getTables(null, schemaName.toUpperCase(), null, null);
            while (resultSet.next()) {
                tableDAO.drop(schemaName, resultSet.getString("TABLE_NAME"), false);
            }

            //
            // Delete procedures
            //
            resultSet = connection.getMetaData().getProcedures(null, schemaName.toUpperCase(), null);
            while (resultSet.next()) {
                jdbcTemplate.executeUpdate(String.format("drop procedure %s.%s", schemaName.toUpperCase(), resultSet.getString("PROCEDURE_NAME")));
            }

            //
            // Drop schema
            //
            resultSet = connection.getMetaData().getSchemas(null, schemaName.toUpperCase());
            while (resultSet.next()) {
                jdbcTemplate.executeUpdate("drop schema " + schemaName + " RESTRICT");
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
