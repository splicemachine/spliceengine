package com.splicemachine.blob;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

/**
 * @author Jeff Cunningham
 *         Date: 5/22/14
 */
public class Utils {

    public static String driver = "org.apache.derby.jdbc.ClientDriver";
    public static String protocol = "jdbc:derby://localhost:1527/";
    public static final String SPLICE_DB = "splicedb";
    private static Properties props = new Properties();
    private static boolean loaded;

    private static synchronized void loadDriver() throws Exception{
        try {
            Class.forName(driver).newInstance();
        } catch (ClassNotFoundException e) {
            System.err.println("\nUnable to load the JDBC driver " + driver);
            System.err.println("Please check your CLASSPATH.");
            e.printStackTrace(System.err);
            throw e;
        } catch (InstantiationException e) {
            System.err.println(
                "\nUnable to instantiate the JDBC driver " + driver);
            e.printStackTrace(System.err);
            throw e;
        } catch (IllegalAccessException e) {
            System.err.println(
                "\nNot allowed to access the JDBC driver " + driver);
            e.printStackTrace(System.err);
            throw e;
        }
        loaded = true;
    }

    public static Connection getConnection() throws Exception {
        if (! loaded)
            loadDriver();
        return DriverManager.getConnection(protocol + SPLICE_DB + ";create=true", props);
    }

    public static List<Integer> insertFiles(Map<String, File> fileMap, String schema, String table, String insertString, int batchSize) throws Exception {
        Connection connection = null;
        PreparedStatement statement = null;

        int batch = 0;
        List<Integer> successes = new ArrayList<Integer>();
        try {
            connection = getConnection();
            statement = connection.prepareStatement(
                String.format(insertString, schema, table));

            int[] success;
            for (Map.Entry<String, File> file : fileMap.entrySet()) {
                InputStream fin = new java.io.FileInputStream(file.getValue());
                statement.setString(1, file.getKey());
                statement.setBinaryStream(2, fin);
                statement.addBatch();
                if (batch % batchSize == 0) {
                    addAll(statement.executeBatch(), successes);
                }
                ++batch;
            }
            if (batch < batchSize) {
                addAll(statement.executeBatch(), successes);
            }
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
        return successes;
    }

    private static void addAll(int[] ints, List<Integer> sink) {
        for (int anInt : ints) {
            sink.add(anInt);
        }
    }

    public static Map<String, File> getTiffFiles(String directoryName, final String tiffExt) {
        File directory = new File(directoryName);
        if (! directory.exists() || ! directory.isDirectory()) {
            throw new IllegalArgumentException("Expecting a directory of "+tiffExt+" files but got: "+directoryName);
        }
        Map<String,File> fileMap = new HashMap<String, File>();
        for (File file : FileUtils.listFiles(directory,
                                             new IOFileFilter() {
                                                 @Override
                                                 public boolean accept(File file) {
                                                     return (file != null && file.getName().endsWith(tiffExt));
                                                 }

                                                 @Override
                                                 public boolean accept(File dir, String name) {
                                                     return false;
                                                 }
                                             }, null
        )) {
            fileMap.put(file.getName().substring(0, file.getName().indexOf('.')), file);
        }

        assertTrue("Found no tiff files in "+directoryName+" with extension "+tiffExt, fileMap.size() > 0);
        return fileMap;
    }

    public static void createTable(String schemaName, String tableName, String tableSchema) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            connection = getConnection();
            rs = connection.getMetaData().getTables(null, schemaName, tableName, null);
            if (rs.next()) {
                dropTable(schemaName, tableName);
            }
            connection.commit();
            statement = connection.createStatement();
            statement.execute(String.format("create table %s.%s %s", schemaName, tableName, tableSchema));
            connection.commit();
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public static void dropTable(String schemaName, String tableName) throws Exception {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection();
            ResultSet rs = connection.getMetaData().getTables(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
            if (rs.next()) {
                statement = connection.createStatement();
                statement.execute(String.format("drop table %s.%s", schemaName.toUpperCase(), tableName.toUpperCase()));
            }
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public static void assertTrue(boolean assertion) {
        assertTrue(null, assertion);
    }

    public static void assertTrue(String failureMsg, boolean assertion) {
        if (!assertion) {
            throw new RuntimeException((failureMsg!=null?failureMsg:"")+" Assertion failed.");
        }
    }

    public static void assertEquals(int num1, int num2) {
        if (! (num1==num2)) {
            throw new RuntimeException("Assertion failed.");
        }
    }
}
