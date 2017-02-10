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

package com.splicemachine.derby.test.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
/**
 *
 *
 */
public class SpliceIndexWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceIndexWatcher.class);
	protected String tableName;
	protected String tableSchemaName;
	protected String createString;
	protected String indexName;
	protected String indexSchemaName;
	protected String create = "create";
	protected static String SELECT_SPECIFIC_INDEX = "select c.conglomeratename from sys.sysconglomerates c inner join sys.sysschemas s on " + 
									"c.schemaid = s.schemaid where c.isindex = 'TRUE' and s.schemaname = ? and c.conglomeratename = ?";
	
	
	public SpliceIndexWatcher(String tableName, String tableSchemaName, String indexName, String indexSchemaName, String createString) {
		this(tableName,tableSchemaName, indexName, indexSchemaName, createString, false);
	}

	public SpliceIndexWatcher(String tableName, String tableSchemaName, String indexName, String indexSchemaName, String createString, boolean unique) {
		this.tableName = tableName.toUpperCase();
		this.tableSchemaName = tableSchemaName.toUpperCase();
		this.createString = createString;
		this.indexName = indexName.toUpperCase();
		this.indexSchemaName = indexSchemaName.toUpperCase();
		if (unique)
			create = create+" unique";
	}

	@Override
	public void starting(Description description) {
		LOG.trace("Starting");
		Connection connection = null;
		PreparedStatement statement = null;
		Statement statement2 = null;
		ResultSet rs = null;
		try {
			connection = SpliceNetConnection.getConnection();
			statement = connection.prepareStatement(SELECT_SPECIFIC_INDEX);
			statement.setString(1, indexSchemaName);
			statement.setString(2, indexName);			
			rs = statement.executeQuery();
			if (rs.next()) {
				executeDrop(connection,indexSchemaName,indexName);
			}
			connection.commit();
			statement2 = connection.createStatement();
			statement2.execute(String.format("%s index %s.%s on %s.%s %s",create,indexSchemaName,indexName,tableSchemaName,tableName,createString));
			connection.commit();
		} catch (Exception e) {
			LOG.error("Create index statement is invalid ");
			throw new RuntimeException(e);
		} finally {
			DbUtils.closeQuietly(rs);
			DbUtils.closeQuietly(statement);
			DbUtils.closeQuietly(statement2);
			DbUtils.commitAndCloseQuietly(connection);
		}
		super.starting(description);
	}
	@Override
	public void finished(Description description) {
		LOG.trace("finished");
//		executeDrop(SpliceNetConnection.indexSchemaName,indexName);
	}

    /**
     * Use this static method in cases where you want to create an index after creating/loading table.
     * TODO: redirect starting(Description) to call this method
     * @param connection
     * @param schemaName
     * @param tableName
     * @param indexName
     * @param definition
     * @param unique
     * @throws Exception
     */
    public static void createIndex(Connection connection, String schemaName, String tableName, String indexName, String definition, boolean unique) throws Exception {
        PreparedStatement statement = null;
        ResultSet rs = null;
        try {
//            connection = SpliceNetConnection.getConnection();
            statement = connection.prepareStatement(SELECT_SPECIFIC_INDEX);
            statement.setString(1, schemaName);
            statement.setString(2, indexName);
            rs = statement.executeQuery();
            if (rs.next()) {
                SpliceIndexWatcher.executeDrop(connection,schemaName,indexName);
            }
			try(Statement s = connection.createStatement()){
				System.out.println(String.format("create "+(unique?"unique":"")+" index %s.%s on %s.%s %s",
						schemaName,indexName,schemaName,tableName,definition));
				s.execute(String.format("create "+(unique?"unique":"")+" index %s.%s on %s.%s %s",
						schemaName,indexName,schemaName,tableName,definition));
			}
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
        }
    }

    public static void executeDrop(Connection connection,String indexSchemaName,String indexName) {
		LOG.trace("executeDrop");
		try(Statement statement = connection.createStatement()) {
			statement.execute(String.format("drop index %s.%s",indexSchemaName.toUpperCase(),indexName.toUpperCase()));
		} catch (Exception e) {
			LOG.error("error Dropping "+e.getMessage());
			throw new RuntimeException(e);
		}
	}

    public void drop() {
//        executeDrop(indexSchemaName,indexName);
    }
}
