/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.BaseJDBCTestSetup;
import com.splicemachine.dbTesting.junit.BaseTestCase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;

/**
 * This class is a decorator for the Scrollable Updatable Resultset
 * tests.  It sets up a datamodel and populates it with data.
 */
public class SURDataModelSetup extends BaseJDBCTestSetup
{  
    /**
     * Constructor.
     * @param test test to decorate with this setup
     * @param model enumerator for which model to use.
     * (Alternatively we could use a subclass for each model)
     */
    public SURDataModelSetup(Test test, SURDataModel model) {
        super(test);
        this.model = model;       
    }

     /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     * @param model enumerator for which model to use
     * @param con connection to database
     * @param records number of records in the data model
     */
    public static void createDataModel(SURDataModel model, Connection con,
                                       int records) 
        throws SQLException
    {
        
        BaseJDBCTestCase.dropTable(con, "T1");
        
        Statement statement = con.createStatement();     
        
        /** Create the table */
        statement.execute(model.getCreateTableStatement());
        BaseTestCase.println(model.getCreateTableStatement());
        
        /** Create secondary index */
        if (model.hasSecondaryKey()) {
            statement.execute("create index a_on_t on t1(a)");
            BaseTestCase.println("create index a_on_t on t1(a)");
        }
        
        /** Populate with data */
        PreparedStatement ps = con.
            prepareStatement("insert into t1 values (?,?,?,?)");
        
        for (int i=0; i<records; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            ps.setInt(3, i*2 + 17);
            ps.setString(4, "Tuple " +i);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
        statement.close();
        con.commit();
    }
    
    /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     * The model will be set up with the number of records as defined by
     * the recordCount attribute.
     * @param model enumerator for which model to use
     * @param con connection to database
     */
    public static void createDataModel(SURDataModel model, Connection con) 
        throws SQLException
    {
        createDataModel(model, con, recordCount);
    }
    
    /**
     * Creates a datamodel for testing Scrollable Updatable ResultSets
     * and populates the database model with data.
     */
    protected void setUp() throws  Exception {       
        println("Setting up datamodel: " + model);

        try {
            Connection con = getConnection();
            con.setAutoCommit(false);
            createDataModel(model, con);
        } catch (SQLException e) {
            printStackTrace(e); // Print the entire stack
            throw e;
        }
    }
    
    /**
     * Delete the datamodel
     */
    protected void tearDown() 
        throws Exception
    {
        try {
            Connection con = getConnection();
            con.rollback();
            con.createStatement().execute("drop table t1");
            con.commit();
        } catch (SQLException e) {
            printStackTrace(e);
        }
        super.tearDown();
    }
    
    public String toString() {
        return "SURDataModel tests with model: " + model;
    }

    private final SURDataModel model;
    final static int recordCount = 10;  // Number of records in data model.  
        
    /**
     * Enum for the layout of the data model
     */
    public final static class SURDataModel {

        /** Model with no keys */
        public final static SURDataModel MODEL_WITH_NO_KEYS = 
            new SURDataModel("NO_KEYS");
        
        /** Model with primary key */
        public final static SURDataModel MODEL_WITH_PK = 
            new SURDataModel("PK");
        
        /** Model with secondary index */
        public final static SURDataModel MODEL_WITH_SECONDARY_KEY = 
            new SURDataModel("SECONDARY_KEY");
        
        /** Model with primary key and secondary index */
        public final static SURDataModel MODEL_WITH_PK_AND_SECONDARY_KEY = 
            new SURDataModel("PK_AND_SECONDARY_KEY");

        /** Array with all values */
        private final static Set values = Collections.unmodifiableSet
            (new HashSet((Arrays.asList(new SURDataModel[] {
                MODEL_WITH_NO_KEYS, 
                MODEL_WITH_PK, 
                MODEL_WITH_SECONDARY_KEY,
                MODEL_WITH_PK_AND_SECONDARY_KEY
            }))));
        
        /**
         * Returns an unmodifyable set of all valid data models
         */ 
        public final static Set values() {
            return values;
        }
       

        /** Returns true if this model has primary key */
        public boolean hasPrimaryKey() {
            return (this==MODEL_WITH_PK || 
                    this==MODEL_WITH_PK_AND_SECONDARY_KEY);
        }
        
        /** Returns true if this model has a secondary key */
        public boolean hasSecondaryKey() {
            return (this==MODEL_WITH_SECONDARY_KEY || 
                    this==MODEL_WITH_PK_AND_SECONDARY_KEY);
        }

        /**
         * Returns the string for creating the table
         */
        public String getCreateTableStatement() {
            return hasPrimaryKey() 
                ? "create table t1 (id int primary key, a int, b int, c varchar(5000))"
                : "create table t1 (id int, a int, b int, c varchar(5000))";
        }

        /**
         * Returns a string representation of the model 
         * @return string representation of this object
         */
        public String toString() {
            return name;
        }
        
        /**
         * Constructor
         */
        private SURDataModel(String name) {
            this.name = name;
        }
        
        
        
        private final String name;
    }

    /**
     * Prints the stack trace. If run in the harness, the
     * harness will mark the test as failed if this method
     * has been called.
     */
    static void printStackTrace(Throwable t) {
        BaseJDBCTestCase.printStackTrace(t);
    }
}
