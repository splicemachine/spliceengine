package com.splicemachine.test_tools;

import org.apache.commons.dbutils.DbUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Create tables and optionally insert data.
 *
 * <pre>
 *
 * new TableBuilder(conn)
 *      .withCreate("create table t1(a int)")
 *      .withInsert("insert into t1 value(?)")
 *      .withRows(new MyRowProvider()).build();
 *
 * </pre>
 *
 * Or reuse the builder to create multiple similar tables:
 *
 * <pre>
 *
 * TableBuilder tb = new TableBuilder(conn)
 *      .withCreate("create table %s (a int)")
 *      .withInsert("insert into %s value(?)")
 *      .withRows(new MyRowProvider()).build();
 *
 * tb.withTableName("t1").create();
 * tb.withTableName("t2").create();
 * tb.withTableName("t3").create();
 *
 * </pre>
 */
public class TableCreator {

    private Connection connection;
    private String tableName;
    private String createSql;
    private String insertSql;
    private Iterable<Iterable<Object>> rowProvider;

    public TableCreator(Connection connection) {
        this.connection = connection;
    }

    public TableCreator withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public TableCreator withCreate(String sql) {
        this.createSql = sql;
        return this;
    }

    public TableCreator withInsert(String sql) {
        this.insertSql = sql;
        return this;
    }

    public TableCreator withRows(Iterable<Iterable<Object>> rowProvider) {
        this.rowProvider = rowProvider;
        return this;
    }

    public void create() throws SQLException {
        createTable();
        if (rowProvider != null) {
            insertRows();
        }
    }

    private void createTable() throws SQLException {
        String CREATE_SQL = tableName == null ? createSql : String.format(createSql, tableName);
        Statement statement = connection.createStatement();
        try {
            statement.execute(CREATE_SQL);
        } finally {
            DbUtils.close(statement);
        }
    }

    private void insertRows() throws SQLException {
        String INSERT_SQL = tableName == null ? insertSql : String.format(insertSql, tableName);
        PreparedStatement ps = connection.prepareStatement(INSERT_SQL);
        try {
            for (Iterable<?> row : rowProvider) {
                int i = 1;
                for (Object value : row) {
                    ps.setObject(i++, value);
                }
                ps.execute();
            }
        } finally {
            DbUtils.close(ps);
        }
    }


}
