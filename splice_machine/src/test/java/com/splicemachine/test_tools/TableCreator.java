/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.test_tools;

import splice.com.google.common.collect.Lists;
import org.apache.commons.dbutils.DbUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static splice.com.google.common.base.Preconditions.checkState;

/**
 * Create tables and optionally insert data.
 *
 * <pre>
 *
 * new TableCreator(conn)
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
 * TableCreator tc = new TableCreator(conn)
 *      .withCreate("create table %s (a int)")
 *      .withInsert("insert into %s value(?)")
 *      .withRows(new MyRowProvider()).build();
 *
 * tc.withTableName("t1").create();
 * tc.withTableName("t2").create();
 * tc.withTableName("t3").create();
 *
 * </pre>
 */
public class TableCreator {

    private Connection connection;
    private String tableName;
    private String createSql;
    private String insertSql;
    private List<String> indexSqlList = Lists.newArrayList();
    private RowCreator rowCreator;
    private String constraints;
    private int queryTimeout = 240;

    public TableCreator(Connection connection) {
        this.connection = connection;
    }

    public TableCreator withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public TableCreator withConstraints(String constraints){
        this.constraints = constraints;
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

    public TableCreator withIndex(String sql) {
        this.indexSqlList.add(sql);
        return this;
    }

    public TableCreator withRows(Iterable<Iterable<Object>> rowProvider) {
        this.rowCreator = new IterableRowCreator(rowProvider);
        return this;
    }

    public TableCreator withRows(RowCreator rowProvider) {
        this.rowCreator = rowProvider;
        return this;
    }

    public void create() throws SQLException {
        createTable();
        createIndexes();
        if (rowCreator != null) {
            checkState(insertSql != null, "must provide insert statement if providing rows");
            insertRows();
        }
    }

    private void createTable() throws SQLException {
        String baseSql = createSql;
        if(constraints!=null){
            int lastParenthesis = baseSql.lastIndexOf(")");
            baseSql = baseSql.substring(0,lastParenthesis)+","+constraints+")";
        }
        String CREATE_SQL=tableName!=null?String.format(baseSql,tableName):baseSql;
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(queryTimeout);
        try {
            statement.execute(CREATE_SQL);
        } finally {
            DbUtils.close(statement);
        }
    }

    private void createIndexes() throws SQLException {
        for (String indexSql : indexSqlList) {
            String INDEX_SQL = tableName == null ? indexSql : String.format(indexSql, tableName);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(queryTimeout);
            try {
                statement.execute(INDEX_SQL);
            } finally {
                DbUtils.close(statement);
            }
        }
    }

    private void insertRows() throws SQLException {
        String insertSql = tableName == null ?this.insertSql: String.format(this.insertSql, tableName);
        int batchSize = rowCreator.batchSize();
        rowCreator.reset();
        try(PreparedStatement ps = connection.prepareStatement(insertSql)) {
            ps.setQueryTimeout(queryTimeout);
            if(batchSize>1){
                int size = 0;
                while(rowCreator.advanceRow()){
                    rowCreator.setRow(ps);
                    ps.addBatch();
                    size++;
                    if((size%batchSize)==0){
                        ps.executeBatch();
                    }
                }
            }else{
                while(rowCreator.advanceRow()){
                    rowCreator.setRow(ps);
                    ps.execute();
                }

            }
        }
    }
}
