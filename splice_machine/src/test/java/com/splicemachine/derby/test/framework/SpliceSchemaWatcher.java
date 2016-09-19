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
