package com.splicemachine.derby.impl.load;

import java.nio.charset.StandardCharsets;

import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.load.ColumnInfo;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.SpliceAdmin;

/**
 * Imports a delimiter-separated file located in HDFS in a parallel way.
 * <p/>
 * When importing data which is contained in HDFS, there is an inherent disconnect
 * between the data locality of any normal file in HDFS, and the data locality of the
 * individual region servers.
 * <p/>
 * <p>Under normal HBase circumstances, one would use HBase's provided bulk-import
 * capabilities, which uses MapReduce to align HFiles with HBase's location and then loads
 * them in one single go. This won't work in Splice's case, however, because each insertion
 * needs to update Secondary indices, validate constraints, and so on and so forth which
 * are not executed when bulk-loading HFiles.
 * <p/>
 * <p>Thus, we must parallelize insertions as much as possible, while still maintaining
 * as much data locality as possible. However, it is not an inherent given that any
 * block location has a corresponding region, nor is it given that any given RegionServer
 * has blocks contained on it. To make matters worse, when a RegionServer <em>does</em>
 * have blocks contained on it, there is no guarantee that the data in those blocks
 * is owned by that specific RegionServer.
 * <p/>
 * <p>There isn't a perfect solution to this problem, unfortunately. This implementation
 * favors situations in which a BlockLocation is co-located with a Region; as a consequence,
 * pre-splitting a Table into regions and spreading those regions out across the cluster is likely
 * to improve the performance of this import process.
 *
 * @author Scott Fines
 */
public class HdfsImport {
    private static final Logger LOG = Logger.getLogger(HdfsImport.class);

    private static final ResultColumnDescriptor[] IMPORT_RESULT_COLUMNS = new GenericColumnDescriptor[]{
        new GenericColumnDescriptor("rowsImported", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("failedRows", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("files", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("dataSize", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("failedLog", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
    };

    private static final ResultColumnDescriptor[] CHECK_RESULT_COLUMNS = new GenericColumnDescriptor[]{
        new GenericColumnDescriptor("rowsChecked", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("failedRows", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("files", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
        new GenericColumnDescriptor("dataSize", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
        new GenericColumnDescriptor("failedLog", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
    };

    /**
     * The SYSCS_UTIL.UPSERT_DATA_FROM_FILE system procedure updates or inserts data from a file to a subset of columns
     * in a table. You choose the subset of columns by specifying insert columns.
     * <p/>
     * The syntax and usage of this procedure is identical to the syntax and usage of the
     * {@link #IMPORT_DATA, SYSCS_UTIL.IMPORT_DATA} system procedure, except that SYSCS_UTIL.UPSERT_DATA_FROM_FILE first
     * determines if the database already contains a record that matches an incoming record:
     * <ol>
     * <li>If a matching record is found in the database, that record is updated with column values from the incoming
     * record.</li>
     * <li>If no matching record is found in the database, the incoming record is added to the database as a new record,
     * exactly as it would be if had you called SYSCS_UTIL.IMPORT_DATA.</li>
     * </ol>
     *
     * @param schemaName The name of the schema of the table in which to import.
     * @param tableName The name of the table in which to import.
     * @param insertColumnList The names, in single quotes, of the columns to import. If this is null, all columns are
     *                         imported.
     * @param fileName Either a single file or a directory. If this is a single file, that file is imported; if this is
     *                 a directory, all of the files in that directory are imported. Note that files can be compressed
     *                 or uncompressed.
     * @param columnDelimiter The character used to separate columns, Specify null if using the comma (,) character as
     *                        your delimiter. <b>NOTE: Use of the backslash (\) character is not supported and will
     *                        generate an error.</b>
     * @param characterDelimiter Specifies which character is used to delimit strings in the imported data. You can
     *                           specify null or the empty string to use the default string delimiter, which is the
     *                           double-quote ("). If your input contains control characters such as newline characters,
     *                           make sure that those characters are embedded within delimited strings. <b>NOTE: To use
     *                           the single quote (') character as your string delimiter, you need to escape that
     *                           character. This means that you specify four quotes ('''') as the value of this
     *                           parameter. This is standard SQL syntax.</b>
     * @param timestampFormat The format of timestamps stored in the file. You can set this to null if there are no
     *                        timestamps in the file, or if the format of any timestamps in the file match the
     *                        java.sql.Timestamp default format, which is: "yyyy-MM-dd HH:mm:ss".
     * @param dateFormat The format of datestamps stored in the file. You can set this to null if there are no date
     *                   columns in the file, or if the format of any dates in the file match pattern: "yyyy-mm-dd".
     * @param timeFormat The format of timeFormats stored in the file. You can set this to null if there are no time
     *                   columns in the file, or if the format of any times in the file match pattern: "hh:mm:ss".
     * @param badRecordsAllowed The number of rejected (bad) records that are tolerated before the import fails. If this
     *                          count of rejected records is reached, the import check terminates.
     *                          <ul>
     *                          <li>If you specify -1 as the value of this parameter, all record import failures are tolerated and logged.</li>
     *                          <li>If you specify 0 as the value of this parameter, the import will fail if even one record is bad.</li>
 *                              </ul>
     * @param badRecordDirectory The directory in which status information is logged. Splice Machine logs status
     *                           information to the [import_file_name].bad file in this directory.
     * @param oneLineRecords whether each line in the import file contains one complete record or records span lines.
     *                       The expected values are <code>true</code> or <code>false</code>. The default, if <code>null</code>
     *                       is given, is <code>true</code>.
     * @param charset the {@link java.nio.charset.StandardCharsets character encoding} of the import file. The default is
     *                {@link java.nio.charset.StandardCharsets#UTF_8 UTF_8}.
     * @param results the procedure's internal result set, unseen by users.
     * @throws SQLException
     */
    public static void UPSERT_DATA_FROM_FILE(String schemaName,
                                             String tableName,
                                             String insertColumnList,
                                             String fileName,
                                             String columnDelimiter,
                                             String characterDelimiter,
                                             String timestampFormat,
                                             String dateFormat,
                                             String timeFormat,
                                             long badRecordsAllowed,
                                             String badRecordDirectory,
                                             String oneLineRecords,
                                             String charset,
                                             ResultSet[] results
    ) throws SQLException {
        doImport(schemaName,
                 tableName,
                 insertColumnList,
                 fileName,
                 columnDelimiter,
                 characterDelimiter,
                 timestampFormat,
                 dateFormat,
                 timeFormat,
                 badRecordsAllowed,
                 badRecordDirectory,
                 oneLineRecords,
                 charset,
                 true,
                 false,
                 results);
    }

    public static void UPSERT_CHECK_DATA_FROM_FILE(String schemaName,
                                                   String tableName,
                                                   String insertColumnList,
                                                   String fileName,
                                                   String columnDelimiter,
                                                   String characterDelimiter,
                                                   String timestampFormat,
                                                   String dateFormat,
                                                   String timeFormat,
                                                   long badRecordsAllowed,
                                                   String badRecordDirectory,
                                                   String oneLineRecords,
                                                   String charset,
                                                   ResultSet[] results
    ) throws SQLException {
        // TODO: JC - CHECK_DATA proc implementations have been removed
        doImport(schemaName,
                 tableName,
                 insertColumnList,
                 fileName,
                 columnDelimiter,
                 characterDelimiter,
                 timestampFormat,
                 dateFormat,
                 timeFormat,
                 badRecordsAllowed,
                 badRecordDirectory,
                 oneLineRecords,
                 charset,
                 true,
                 true,
                 results);
    }

    /**
     * The SYSCS_UTIL.IMPORT_DATA system procedure imports data to a subset of columns in a table. You choose the subset
     * of columns by specifying insert columns.
     * <p/>
     * After a successful import completes, a simple report displays, showing how many files were imported, and how many
     * record imports succeeded or failed.
     *
     * @param schemaName The name of the schema of the table in which to import.
     * @param tableName The name of the table in which to import.
     * @param insertColumnList The names, in single quotes, of the columns to import. If this is null, all columns are
     *                         imported.
     * @param fileName Either a single file or a directory. If this is a single file, that file is imported; if this is
     *                 a directory, all of the files in that directory are imported. Note that files can be compressed
     *                 or uncompressed.
     * @param columnDelimiter The character used to separate columns, Specify null if using the comma (,) character as
     *                        your delimiter. <b>NOTE: Use of the backslash (\) character is not supported and will
     *                        generate an error.</b>
     * @param characterDelimiter Specifies which character is used to delimit strings in the imported data. You can
     *                           specify null or the empty string to use the default string delimiter, which is the
     *                           double-quote ("). If your input contains control characters such as newline characters,
     *                           make sure that those characters are embedded within delimited strings. <b>NOTE: To use
     *                           the single quote (') character as your string delimiter, you need to escape that
     *                           character. This means that you specify four quotes ('''') as the value of this
     *                           parameter. This is standard SQL syntax.</b>
     * @param timestampFormat The format of timestamps stored in the file. You can set this to null if there are no
     *                        timestamps in the file, or if the format of any timestamps in the file match the
     *                        java.sql.Timestamp default format, which is: "yyyy-MM-dd HH:mm:ss".
     * @param dateFormat The format of datestamps stored in the file. You can set this to null if there are no date
     *                   columns in the file, or if the format of any dates in the file match pattern: "yyyy-mm-dd".
     * @param timeFormat The format of timeFormats stored in the file. You can set this to null if there are no time
     *                   columns in the file, or if the format of any times in the file match pattern: "hh:mm:ss".
     * @param badRecordsAllowed The number of rejected (bad) records that are tolerated before the import fails. If this
     *                          count of rejected records is reached, the import check terminates.
     *                          <ul>
     *                          <li>If you specify -1 as the value of this parameter, all record import failures are tolerated and logged.</li>
     *                          <li>If you specify 0 as the value of this parameter, the import will fail if even one record is bad.</li>
     *                          </ul>
     * @param badRecordDirectory The directory in which status information is logged. Splice Machine logs status
     *                           information to the [import_file_name].bad file in this directory.
     * @param oneLineRecords whether each line in the import file contains one complete record or records span lines.
     *                       The expected values are <code>true</code> or <code>false</code>. The default, if <code>null</code>
     *                       is given, is <code>true</code>.
     * @param charset the {@link java.nio.charset.StandardCharsets character encoding} of the import file. The default is
     *                {@link java.nio.charset.StandardCharsets#UTF_8 UTF_8}.
     * @param results the procedure's internal result set, unseen by users.
     * @throws SQLException
     */
    public static void IMPORT_DATA(String schemaName,
                                   String tableName,
                                   String insertColumnList,
                                   String fileName,
                                   String columnDelimiter,
                                   String characterDelimiter,
                                   String timestampFormat,
                                   String dateFormat,
                                   String timeFormat,
                                   long badRecordsAllowed,
                                   String badRecordDirectory,
                                   String oneLineRecords,
                                   String charset,
                                   ResultSet[] results
    ) throws SQLException {
        doImport(schemaName,
                 tableName,
                 insertColumnList,
                 fileName,
                 columnDelimiter,
                 characterDelimiter,
                 timestampFormat,
                 dateFormat,
                 timeFormat,
                 badRecordsAllowed,
                 badRecordDirectory,
                 oneLineRecords,
                 charset,
                 false,
                 false,
                 results);
    }

    public static void IMPORT_CHECK_DATA(String schemaName,
                                         String tableName,
                                         String insertColumnList,
                                         String fileName,
                                         String columnDelimiter,
                                         String characterDelimiter,
                                         String timestampFormat,
                                         String dateFormat,
                                         String timeFormat,
                                         long badRecordsAllowed,
                                         String badRecordDirectory,
                                         String oneLineRecords,
                                         String charset,
                                         ResultSet[] results
    ) throws SQLException {
        // TODO: JC - CHECK_DATA proc implementations have been removed
        doImport(schemaName,
                 tableName,
                 insertColumnList,
                 fileName,
                 columnDelimiter,
                 characterDelimiter,
                 timestampFormat,
                 dateFormat,
                 timeFormat,
                 badRecordsAllowed,
                 badRecordDirectory,
                 oneLineRecords,
                 charset,
                 false,
                 true,
                 results);
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION",justification = "Intentional")
    private static void doImport(String schemaName,
                                 String tableName,
                                 String insertColumnList,
                                 String fileName,
                                 String columnDelimiter,
                                 String characterDelimiter,
                                 String timestampFormat,
                                 String dateFormat,
                                 String timeFormat,
                                 long badRecordsAllowed,
                                 String badRecordDirectory,
                                 String oneLineRecords,
                                 String charset,
                                 boolean isUpsert,
                                 boolean isCheckScan,
                                 ResultSet[] results) throws SQLException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "doImport {schemaName=%s, tableName=%s, insertColumnList=%s, fileName=%s, " +
                                     "columnDelimiter=%s, characterDelimiter=%s, timestampFormat=%s, dateFormat=%s, " +
                "timeFormat=%s, badRecordsAllowed=%d, badRecordDirectory=%s, oneLineRecords=%s, charset=%s, " +
                "isUpsert=%s, isCheckScan=%s}",
                                 schemaName, tableName, insertColumnList, fileName, columnDelimiter, characterDelimiter,
                                 timestampFormat, dateFormat, timeFormat, badRecordsAllowed, badRecordDirectory,
                                 oneLineRecords, charset, isUpsert, isCheckScan);

        if (charset == null)
            charset = StandardCharsets.UTF_8.name();

        Connection conn = null;
        try {
            schemaName = EngineUtils.validateSchema(schemaName);
            tableName = EngineUtils.validateTable(tableName);
            conn = SpliceAdmin.getDefaultConn();
            // This needs to be found by the database locale, not hard coded.
            if (timestampFormat == null)
                timestampFormat = "yyyy-MM-dd HH:mm:ss";
            if (dateFormat == null)
                dateFormat = "yyyy-MM-dd";
            if (timeFormat == null)
                timeFormat = "HH:mm:ss";

            String importVTI = "new " + "com.splicemachine.derby.vti.SpliceFileVTI" +
                "(" +
                quoteStringArgument(fileName) +
                "," +
                quoteStringArgument(characterDelimiter) +
                "," +
                quoteStringArgument(columnDelimiter) +
                "," +
                quoteStringArgument(null) +
                "," +
                quoteStringArgument(timeFormat) +
                "," +
                quoteStringArgument(dateFormat) +
                "," +
                quoteStringArgument(timestampFormat) +
                "," +
                quoteStringArgument((oneLineRecords == null ? "TRUE" : oneLineRecords.toLowerCase())) +
                "," +
                quoteStringArgument(charset) +
                " )";
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);
            if (insertColumnList != null && (insertColumnList.isEmpty() || insertColumnList.toLowerCase().equals("null")) )
                insertColumnList = null;

            ColumnInfo columnInfo = new ColumnInfo(conn, schemaName, tableName, insertColumnList != null ?
                insertColumnList.toUpperCase() :null);
            String insertSql = "INSERT INTO " + entityName + "(" + columnInfo.getInsertColumnNames() + ") " +
                "--splice-properties insertMode=" + (isUpsert ? "UPSERT" : "INSERT") + ", statusDirectory=" +
                badRecordDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n" +
                " SELECT * from " +
                importVTI + " AS importVTI (" + columnInfo.getImportAsColumns() + ")";

            //prepare the import statement to hit any errors before locking the table
            //execute the import operation.
            try (PreparedStatement ips = conn.prepareStatement(insertSql)) {
                FileInfo contentSummary = ImportUtils.getImportFileInfo(fileName);
                int count = ips.executeUpdate();
                String badFileName = ((EmbedConnection) conn).getLanguageConnection().getBadFile();
                ExecRow result = new ValueRow(5);
                result.setRowArray(new DataValueDescriptor[]{
                    new SQLLongint(count),
                    new SQLLongint(((EmbedConnection) conn).getLanguageConnection().getFailedRecords()),
                    new SQLLongint(contentSummary.fileCount()),
                    new SQLLongint(contentSummary.size()),
                    new SQLVarchar((badFileName == null || badFileName.isEmpty() ? "NONE" : badFileName))
                });
                Activation act = ((EmbedConnection) conn).getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet rs =
                    new IteratorNoPutResultSet(Collections.singletonList(result),
                                               (isCheckScan ? CHECK_RESULT_COLUMNS : IMPORT_RESULT_COLUMNS), act);
                rs.open();
                results[0] = new EmbedResultSet40((EmbedConnection) conn, rs, false, null, true);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    /**
     * Quote a string argument so that it can be used as a literal in an
     * SQL statement. If the string argument is {@code null} an SQL NULL token
     * is returned.
     *
     * @param string a string or {@code null}
     * @return the string in quotes and with proper escape sequences for
     * special characters, or "NULL" if the string is {@code null}
     */
    private static String quoteStringArgument(String string) {
        if (string == null) {
            return "NULL";
        }
        return StringUtil.quoteStringLiteral(string);
    }
}
