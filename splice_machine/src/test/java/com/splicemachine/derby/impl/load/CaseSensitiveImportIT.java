package com.splicemachine.derby.impl.load;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sparkproject.guava.collect.Lists;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * This test exists to test case-sensitive identifiers because the test framework, SpliceSchemaWatcher,
 * SpliceTableWatcher, etc., uppercase identifiers.
 */
public class CaseSensitiveImportIT {
//    public static final String SCHEMA_NAME = "\""+CaseSensitiveImportIT.class.getSimpleName()+"\"";
    public static final String SCHEMA_NAME = CaseSensitiveImportIT.class.getSimpleName().toUpperCase();

    private static File BADDIR;

    public static SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void beforeClass() throws Exception {
        cleanSchema(SCHEMA_NAME, methodWatcher);
        methodWatcher.executeUpdate(String.format("create schema %s",SCHEMA_NAME));
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME);
        assertNotNull(BADDIR);
    }

    @Test
    public void testCaseSensitiveTableQuoted() throws Exception {
        String tableName = "\"MixedCase\"";

        methodWatcher.executeUpdate(String.format("create table %s ",SCHEMA_NAME+"."+tableName)+ "(i int primary key)");

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
                                            SpliceUnitTest.getResourceDirectory() + "values.txt",
                                            BADDIR.getCanonicalPath());
//        System.out.println(importString);
        PreparedStatement ps = methodWatcher.prepareStatement(importString);
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s.%s", SCHEMA_NAME, tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            results.add(rs.getInt(1)+"");
        }
        Assert.assertEquals("Bad row count.", 10, results.size());
    }

    @Test
    public void testCaseSensitiveInsertColumnListQuoted() throws Exception {
        String tableName = "\"MixedCaseCols\"";

        methodWatcher.executeUpdate( String.format("create table %s ",SCHEMA_NAME+"."+tableName)+
                                         "(\"ColOne\" int primary key, \"ColTwo\" varchar(10))");

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
//        System.out.println(importString);
        PreparedStatement ps = methodWatcher.prepareStatement(importString);
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s.%s", SCHEMA_NAME,
                                                         tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            results.add(rs.getInt(1)+"");
        }
        Assert.assertEquals("Bad row count.", 10, results.size());
    }

    @Test
    public void testCaseSensitiveInsertColumnListWithCommasQuoted() throws Exception {
        String tableName = "\"InCase\"";

        methodWatcher.executeUpdate( String.format("create table %s ",SCHEMA_NAME+"."+tableName)+
                                         "(\"Col,One\" int primary key, \"Col,Two\" varchar(10))");

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
//        System.out.println(importString);
        PreparedStatement ps = methodWatcher.prepareStatement(importString);
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s.%s", SCHEMA_NAME,
                                                         tableName));
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            results.add(rs.getInt(1)+"");
        }
        Assert.assertEquals("Bad row count.", 10, results.size());
    }

    private static void cleanSchema(String schemaName, SpliceWatcher watcher) throws Exception {

        try (Connection connection = watcher.getOrCreateConnection()) {
            DatabaseMetaData metaData=connection.getMetaData();
//            System.out.println("Supports mixed case? "+metaData.supportsMixedCaseIdentifiers());
//            System.out.println("Supports quoted mixed case? "+metaData.supportsMixedCaseQuotedIdentifiers());

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

