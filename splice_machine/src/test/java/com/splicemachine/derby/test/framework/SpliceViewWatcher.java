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

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Jeff Cunningham
 *         Date: 6/7/13
 */
public class SpliceViewWatcher extends TestWatcher {
    public static final String CREATE_VIEW = "create view ";
    private static final Logger LOG = Logger.getLogger(SpliceViewWatcher.class);
    protected String viewName;
    protected String schemaName;
    protected String createString;
    protected String userName;
    protected String password;
    
    /**
     *
     * @param viewName name for the view.
     * @param schemaName schema in which to place the view;
     * @param createString view creation string
     */
    public SpliceViewWatcher(String viewName,String schemaName, String createString) {
        this.viewName = viewName.toUpperCase();
        this.schemaName = schemaName.toUpperCase();
        this.createString = createString;
    }

    public SpliceViewWatcher(String viewName,String schemaName, String createString, String userName, String password) {
    	this(viewName,schemaName,createString);
    	this.userName = userName;
    	this.password = password;
    }

    
    @Override
    public void starting(Description description) {
        LOG.trace("Starting");
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            connection = userName==null?SpliceNetConnection.getConnection():SpliceNetConnection.getConnectionAs(userName,password);
            rs = connection.getMetaData().getTables(null, schemaName, viewName, null);
            if (rs.next()) {
                executeDrop(schemaName, viewName);
            }
            connection.commit();
            statement = connection.createStatement();
            statement.execute(CREATE_VIEW + schemaName + "." + viewName + " " + createString);
            connection.commit();
        } catch (Exception e) {
            LOG.error("Create view statement is invalid ");
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
        super.starting(description);
    }
    @Override
    public void finished(Description description) {
        LOG.trace("finished");
//        executeDrop(schemaName, viewName);
    }

    public static void executeDrop(String schemaName,String viewName) {
        LOG.trace("executeDrop");
        Connection connection = null;
        Statement statement = null;
        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.createStatement();
            statement.execute("drop view " + schemaName.toUpperCase() + "." + viewName.toUpperCase());
            connection.commit();
        } catch (Exception e) {
            LOG.error("error Dropping " + e.getMessage());
            e.printStackTrace(System.err);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    @Override
    public String toString() {
        return schemaName+"."+viewName;
    }

}
