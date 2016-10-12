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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.spark_project.guava.collect.Lists;

import java.io.File;
import java.sql.*;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * This test exists to test case-sensitive identifiers because the test framework, SpliceSchemaWatcher,
 * SpliceTableWatcher, etc., uppercase identifiers.
 */
public class CaseSensitiveImportIT {
    public static final String SCHEMA_NAME = CaseSensitiveImportIT.class.getSimpleName().toUpperCase();

    private static File BADDIR;

    public static SpliceWatcher methodWatcher = new SpliceWatcher();

    private TestConnection conn;

    @BeforeClass
    public static void beforeClass() throws Exception {
        cleanSchema(SCHEMA_NAME, methodWatcher);
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            s.executeUpdate(String.format("create schema %s",SCHEMA_NAME));
        }
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME);
        assertNotNull(BADDIR);
    }

    @Before
    public void setUp() throws Exception{
       conn = methodWatcher.getOrCreateConnection();
    }

    @Test
    public void testCaseSensitiveTableQuoted() throws Exception {
        String tableName = "\"MixedCase\"";

        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s ",SCHEMA_NAME+"."+tableName)+"(i int primary key)");
        }

            String importString=String.format("call SYSCS_UTIL.IMPORT_DATA("+
                            "'%s',"+  // schema name
                            "'%s',"+  // table name
                            "null,"+  // insert column list
                            "'%s',"+  // file path
                            "',',"+   // column delimiter
                            "null,"+  // character delimiter
                            "null,"+  // timestamp format
                            "null,"+  // date format
                            "null,"+  // time format
                            "0,"+    // max bad records
                            "'%s',"+  // bad record dir
                            "null,"+  // has one line records
                            "null)",   // char set
                    SCHEMA_NAME,tableName,
                    SpliceUnitTest.getResourceDirectory()+"values.txt",
                    BADDIR.getCanonicalPath());

        try(PreparedStatement ps = conn.prepareStatement(importString)){
            ps.execute();
        }

        List<String> results=dumpTable(tableName);
        Assert.assertEquals("Bad row count.",10,results.size());
    }

    @Test
    public void testCaseSensitiveInsertColumnListQuoted() throws Exception {
        String tableName = "\"MixedCaseCols\"";

        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s ",SCHEMA_NAME+"."+tableName)+
                    "(\"ColOne\" int primary key, \"ColTwo\" varchar(10))");
        }

        String importString = String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                                                "'%s'," +  // schema name
                                                "'%s'," +  // table name
                                                "'%s'," +  // insert column list
                                                "'%s'," +  // file path
                                                "','," +   // column delimiter
                                                "null," +  // character delimiter
                                                "null," +  // timestamp format
                                                "null," +  // date format
                                                "null," +  // time format
                                                "0," +    // max bad records
                                                "'%s'," +  // bad record dir
                                                "null," +  // has one line records
                                                "null)",   // char set
                                            SCHEMA_NAME, tableName,
                                            "\"ColOne\",\"ColTwo\"",
                                            SpliceUnitTest.getResourceDirectory() + "valuesTwo.txt",
                                            BADDIR.getCanonicalPath());

        try(PreparedStatement ps = conn.prepareStatement(importString)){
            ps.execute();
        }

        List<String> results=dumpTable(tableName);
        Assert.assertEquals("Bad row count.",10,results.size());
    }

    @Test
    public void testCaseSensitiveColumnsNoColumnList() throws Exception {
        /*
         * This doesn't really test the import procedure itself so much as it tests the setup
         * of the import (that we properly find all the mixed-case columns, etc.). It mainly
         * validated SPLICE-1049 is not still a problem.
         */
        String tableName = "\"MixedCaseCols2\"";

        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s ",SCHEMA_NAME+"."+tableName)+
                    "(\"ColOne\" int primary key, \"ColTwo\" varchar(10),\"colthree\" varchar(10))");
        }

        String importString = String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "0," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                SCHEMA_NAME, tableName,
                SpliceUnitTest.getResourceDirectory() + "import/valuesThree.txt",
                BADDIR.getCanonicalPath());

        try(PreparedStatement ps = conn.prepareStatement(importString)){
            ps.execute();
        }

        List<String> results=dumpTable(tableName);
        Assert.assertEquals("Bad row count.",10,results.size());
    }

    @Test
    public void testCaseSensitiveInsertColumnListWithCommasQuoted() throws Exception {
        String tableName = "\"InCase\"";

        try(Statement s = conn.createStatement()){
            s.executeUpdate(String.format("create table %s ",SCHEMA_NAME+"."+tableName)+
                    "(\"Col,One\" int primary key, \"Col,Two\" varchar(10))");
        }

        String importString = String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                                                "'%s'," +  // schema name
                                                "'%s'," +  // table name
                                                "'%s'," +  // insert column list
                                                "'%s'," +  // file path
                                                "','," +   // column delimiter
                                                "null," +  // character delimiter
                                                "null," +  // timestamp format
                                                "null," +  // date format
                                                "null," +  // time format
                                                "0," +    // max bad records
                                                "'%s'," +  // bad record dir
                                                "null," +  // has one line records
                                                "null)",   // char set
                                            SCHEMA_NAME, tableName,
                                            "\"Col,One\",\"Col,Two\"",
                                            SpliceUnitTest.getResourceDirectory() + "valuesTwo.txt",
                                            BADDIR.getCanonicalPath());
        try(PreparedStatement ps = conn.prepareStatement(importString)){
            ps.execute();
        }

        List<String> results=dumpTable(tableName);
        Assert.assertEquals("Bad row count.",10,results.size());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private List<String> dumpTable(String tableName) throws SQLException{
        try(Statement s = conn.createStatement()){
            List<String> results=Lists.newArrayList();
            try(ResultSet rs=s.executeQuery(String.format("select * from %s.%s",SCHEMA_NAME,tableName))){
                while(rs.next()){
                    results.add(rs.getInt(1)+"");
                }
            }
            return results;
        }
    }

    private static void cleanSchema(String schemaName, SpliceWatcher watcher) throws Exception {

        try (Connection connection = watcher.getOrCreateConnection()) {
            DatabaseMetaData metaData=connection.getMetaData();

            //
            // Deletes tables
            //
            try(ResultSet resultSet = metaData.getTables(null,schemaName,null,null)){
                while(resultSet.next()){
                    watcher.executeUpdate(String.format("drop table %s.%s",schemaName,"\""+resultSet.getString("TABLE_NAME")+"\""));
                }
            }

            //
            // Drop schema
            //
            try(ResultSet resultSet = metaData.getSchemas(null,schemaName)){
                while(resultSet.next()){
                    watcher.executeUpdate("drop schema "+schemaName+" RESTRICT");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

