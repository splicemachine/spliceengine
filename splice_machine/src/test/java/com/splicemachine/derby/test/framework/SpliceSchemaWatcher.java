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

import com.splicemachine.test_dao.SchemaDAO;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SpliceSchemaWatcher extends TestWatcher {

    private static final Logger LOG = Logger.getLogger(SpliceSchemaWatcher.class);

    public String schemaName;
    protected String userName;

    public SpliceSchemaWatcher(String schemaName) {
        this.schemaName = schemaName.toUpperCase();
    }

    public SpliceSchemaWatcher(String schemaName, String userName) {
        this(schemaName);
        this.userName = userName;
    }

    @Override
    protected void starting(Description description) {
        try (Connection connection = SpliceNetConnection.getConnection()){
//            connection.setAutoCommit(false);

            SchemaDAO schemaDAO = new SchemaDAO(connection);
            try(ResultSet rs = connection.getMetaData().getSchemas(null, schemaName)){
                if(rs.next()){
                    schemaDAO.drop(rs.getString("TABLE_SCHEM"));
                }
            }catch(Exception e){
                e.printStackTrace();
                connection.rollback();
                throw e;
            }
            try(Statement statement = connection.createStatement()){
                if(userName!=null)
                    statement.execute(String.format("create schema %s AUTHORIZATION %S",schemaName,userName));
                else
                    statement.execute(String.format("create schema %s",schemaName));
            }catch(Exception e){
                e.printStackTrace();
                connection.rollback();
                throw e;
            }
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        super.starting(description);
    }

    @Override
    protected void finished(Description description) {
        LOG.trace(tag("Finished", schemaName));
    }

    @Override
    public String toString() {
        return schemaName;
    }

    //-----------------------------------------------------------------------------------------
    // The following methods are for tagging the log messages with additional information
    // related to the schema and table.
    //-----------------------------------------------------------------------------------------

    /**
     * Tag the message with extra information (schema name) if the message is a String.
     *
     * @param message message to be potentially tagged
     * @param schema  name of schema
     */
    protected static Object tag(Object message, String schema) {
        if (message instanceof String) {
            return String.format("[%s] %s", schema, message);
        } else {
            return message;
        }
    }
}
