package com.splicemachine.blob;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;

/**
 * @author Jeff Cunningham
 *         Date: 5/22/14
 */
public class RunTiffBlobs {

    public static final String DEFAULT_TIFF_EXT = ".tif";
    public static final String DEFAULT_SCHEMA_NAME = "BLOB_TEST";
    public static final String DEFAULT_TABLE_NAME = "INVOICES";
    public static final String DEFAULT_TABLE_SCHEMA = "(name varchar(32) not null primary key, invoice blob(50M))";
    public static final int DEFAULT_BATCH_INSERT_SIZE = 10;

    private static final String TIFF_INSERT = "insert into %s.%s (name, invoice) values (?,?)";
    private static final String TIFF_SELECT = "select invoice from %s.%s where name = '%s'";

    public void createInvoicesTable(String schemaName, String tableName, String tableSchema) throws Exception {
        Utils.createTable(schemaName, tableName, tableSchema);
    }

    public void dropInvoicesTable(String schemaName, String tableName) throws Exception {
        Utils.dropTable(schemaName, tableName);
    }

    public Map<String, File> getFiles(String directoryName, String tiffFileExt) throws Exception {
        return Utils.getTiffFiles(directoryName, tiffFileExt);
    }

    public Map<String, File> insertTiffFiles(String schemaName, String tableName, int insertBatchSize,
                                             Map<String, File> fileMap) throws Exception {
        Utils.assertTrue(Utils.insertFiles(fileMap,schemaName,tableName, TIFF_INSERT,insertBatchSize).size() == fileMap.size());
        return fileMap;
    }

    public void validateTiffBlobStream(String schemaName, String tableName, Map<String, File> fileMap) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            connection = Utils.getConnection();
            statement = connection.createStatement();

            int i =0;
            for (Map.Entry<String, File> file : fileMap.entrySet()) {
                resultSet = statement.executeQuery(
                    String.format(TIFF_SELECT, schemaName, tableName, file.getKey()));
                byte buff[] = new byte[1024];
                while (resultSet.next()) {
                    String filename = "/tmp/" + file.getValue().getName();
                    Blob blob = resultSet.getBlob(1);
                    File newFile = new File(filename);
                    if (newFile.exists()) {
                        Utils.assertTrue(newFile.delete());
                    }
                    Utils.assertTrue(newFile.createNewFile());
                    InputStream is = blob.getBinaryStream();
                    FileOutputStream fos = new FileOutputStream(newFile);
                    for (int b = is.read(buff); b != -1; b = is.read(buff)) {
                        fos.write(buff, 0, b);
                    }
                    is.close();
                    fos.close();

                    File origFile = file.getValue();
                    Utils.assertTrue("The files contents are not equivalent", FileUtils.contentEquals(origFile, newFile));
                    ++i;
                }
            }
            Utils.assertEquals(fileMap.size(),i);
        } finally {
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(resultSet);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    public static void main(String[] args) {
       RunTiffBlobs vts = new RunTiffBlobs();
        String tiffFileExt = DEFAULT_TIFF_EXT;

        if (args == null || args.length == 0) {
            usage(System.err, "Missing args");
            System.exit(1);
        }
        try {
            for (int i=0; i<args.length; ++i) {
                String arg = args[i];
                if (arg.equalsIgnoreCase("-h") ) {
                    usage(System.out, null);
                    System.exit(0);
                }
                if (arg.equalsIgnoreCase("-e") ) {
                    tiffFileExt = getNextArgOrFail(args, i, "Expected -e <tiff_file_extension>");
                    System.out.println("Tiff file ext override: "+tiffFileExt);
                }
                if (arg.equalsIgnoreCase("-create") ) {
                    System.out.println("Creating "+DEFAULT_SCHEMA_NAME+"."+DEFAULT_TABLE_NAME);
                    vts.createInvoicesTable(DEFAULT_SCHEMA_NAME,DEFAULT_TABLE_NAME,DEFAULT_TABLE_SCHEMA);
                    System.exit(0);
                }
                if (arg.equalsIgnoreCase("-load") ) {
                    String directory = getNextArgOrFail(args, i, "Expected -load <directory_path>");
                    Map<String,File> files = vts.getFiles(directory, tiffFileExt);
                    System.out.println("Inserting "+files.size()+" tiff files into "+DEFAULT_SCHEMA_NAME+"."+DEFAULT_TABLE_NAME);
                    vts.insertTiffFiles(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, DEFAULT_BATCH_INSERT_SIZE, files);
                    System.exit(0);
                }
                if (arg.equalsIgnoreCase("-validate") ) {
                    String directory = getNextArgOrFail(args, i, "Expected -validate <directory_path>");
                    Map<String,File> files = vts.getFiles(directory, tiffFileExt);
                    System.out.println("Validating "+files.size()+" tiff file contents from DB to those in "+directory);
                    vts.validateTiffBlobStream(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, files);
                    System.exit(0);
                }
                if (arg.equalsIgnoreCase("-drop") ) {
                    System.out.println("Dropping "+DEFAULT_SCHEMA_NAME+"."+DEFAULT_TABLE_NAME);
                    vts.dropInvoicesTable(DEFAULT_SCHEMA_NAME,DEFAULT_TABLE_NAME);
                    System.exit(0);
                }
            }
        } catch (Exception e) {
            usage(System.err, "Caught "+e.getClass().getName());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public static String getNextArgOrFail(String[] args, int i, String errMsg) {
        if (args.length == i+1) {
            usage(System.err, errMsg);
            System.exit(1);
        }
        return args[i+1];
    }

    private static void usage(PrintStream dest, String msg) {
        if (msg != null && ! msg.isEmpty()) {
            dest.println(msg);
            dest.println();
        }
        String usage =
            "-e <tiff_file_extension> : Default is " + DEFAULT_TIFF_EXT + "\n" +
            "-create : Create the "+DEFAULT_TABLE_NAME+" table as " + DEFAULT_TABLE_SCHEMA + "\n" +
            "-load <directory> : Load all tiff files in <directory>. Expects table to have been created.\n" +
            "-validate <directory> : Validate that all tiff BLOBs in the database match file content of\n"+
            "                        tiffs in <directory>. Expects table created and loaded.\n" +
            "-drop : Drop the "+DEFAULT_TABLE_NAME+" table.\n" +
            "-h : This usage message.\n" +
            "";
        dest.println(usage);
    }
}
