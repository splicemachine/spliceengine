/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.load;

import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.catalog.DefaultInfo;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.load.ColumnInfo;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static final ResultColumnDescriptor[] MERGE_RESULT_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("rowsAffected", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("rowsUpdated", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("rowsInserted", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("failedRows", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("files", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("dataSize", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("failedLog", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
    };

    public static void COMPUTE_SPLIT_KEY(String schemaName,
                                         String tableName,
                                         String indexName,
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
                                         String outputDirectory,
                                         ResultSet[] results
    ) throws SQLException {
        if(outputDirectory == null) {
            throw new SQLException(StandardException.newException(SQLState.PARAMETER_CANNOT_BE_NULL, "outputDirectory"));
        }

        doImport(schemaName,
                tableName,
                indexName,
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
                false,
                false,
                outputDirectory,
                null,
                "TRUE",
                null,
                false,
                results);
    }

    public static void SAMPLE_DATA(String schemaName,
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
                                   String bulkImportDirectory,
                                   ResultSet[] results
    ) throws SQLException {
        if(bulkImportDirectory == null) {
            throw new SQLException(StandardException.newException(SQLState.PARAMETER_CANNOT_BE_NULL, "bulkImportDirectory"));
        }
        doImport(schemaName,
                tableName,
                null,
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
                false,
                false,
                bulkImportDirectory,
                "TRUE",
                null,
                "FALSE",
                false,
                results);
    }

    /**
     *
     * @param schemaName
     * @param tableName
     * @param insertColumnList
     * @param fileName
     * @param columnDelimiter
     * @param characterDelimiter
     * @param timestampFormat
     * @param dateFormat
     * @param timeFormat
     * @param badRecordsAllowed
     * @param badRecordDirectory
     * @param oneLineRecords
     * @param charset
     * @param bulkImportDirectory
     * @param results
     * @throws SQLException
     */
    public static void BULK_IMPORT_HFILE(String schemaName,
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
                                         String bulkImportDirectory,
                                         String skipSampling,
                                         ResultSet[] results
    ) throws SQLException {
        if(bulkImportDirectory == null) {
            throw new SQLException(StandardException.newException(SQLState.PARAMETER_CANNOT_BE_NULL, "bulkImportDirectory"));
        }
        doImport(schemaName,
                tableName,
                null,
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
                false,
                false,
                bulkImportDirectory,
                null,
                null,
                skipSampling,
                false,
                results);
    }
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
                null,
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
                false,
                false,
                null,
                null,
                null,
                null,
                false,
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
                null,
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
                false,
                false,
                null,
                null,
                null,
                null,
                false,
                results);
    }

    public static void IMPORT_DATA_UNSAFE(String schemaName,
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
                null,
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
                true,
                true,
                null,
                null,
                null,
                null,
                false,
                results);
    }


    public static void MERGE_DATA_FROM_FILE(String schemaName,
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
                null,
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
                false,
                false,
                null,
                null,
                null,
                null,
                true,
                results);
    }

    @SuppressFBWarnings(value="SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING", justification = "no sql injection")
    private static void doImport(String schemaName,
                                 String tableName,
                                 String indexName,
                                 String insertColumnListString,
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
                                 boolean skipConflictDetection,
                                 boolean skipWAL,
                                 String bulkImportDirectory,
                                 String samplingOnly,
                                 String outputKeysOnly,
                                 String skipSampling,
                                 boolean isMerge,
                                 ResultSet[] results) throws SQLException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "doImport {schemaName=%s, tableName=%s, insertColumnList=%s, fileName=%s, " +
                                     "columnDelimiter=%s, characterDelimiter=%s, timestampFormat=%s, dateFormat=%s, " +
                "timeFormat=%s, badRecordsAllowed=%d, badRecordDirectory=%s, oneLineRecords=%s, charset=%s, " +
                "isUpsert=%s, isCheckScan=%s, skipConflictDetection=%b, skipWAL=%b, isMerge=%b}",
                                 schemaName, tableName, insertColumnListString, fileName, columnDelimiter, characterDelimiter,
                                 timestampFormat, dateFormat, timeFormat, badRecordsAllowed, badRecordDirectory,
                                 oneLineRecords, charset, isUpsert, isCheckScan, skipConflictDetection, skipWAL, isMerge);

        if (charset == null) {
            charset = StandardCharsets.UTF_8.name();
        } else {
            try {
                Charset.forName(charset);
            } catch (UnsupportedCharsetException e) {
                throw PublicAPI.wrapStandardException(ErrorState.LANG_INVALID_CHARACTER_ENCODING.newException(charset));
            }
        }

        // unescape separator chars if need be
        try {
            characterDelimiter = ImportUtils.unescape(characterDelimiter);
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(ErrorState.ILLEGAL_DELIMITER_CHAR.newException("character", characterDelimiter));
        }
        try {
            columnDelimiter = ImportUtils.unescape(columnDelimiter);
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(ErrorState.ILLEGAL_DELIMITER_CHAR.newException("column", columnDelimiter));
        }

        if (columnDelimiter != null && ! columnDelimiter.equals("NULL") && columnDelimiter.equals(characterDelimiter)) {
            throw PublicAPI.wrapStandardException(ErrorState.DELIMITERS_SAME.newException());
        }

        Connection conn = null;
        try {
            if (schemaName == null) {
                schemaName = EngineUtils.getCurrentSchema();
            } else {
                schemaName = EngineUtils.validateSchema(schemaName);
            }
            if (tableName == null) {
                throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
            } else {
                tableName = EngineUtils.validateTable(tableName);
            }
            conn = SpliceAdmin.getDefaultConn();
            // This needs to be found by the database locale, not hard coded.
            if (timestampFormat == null)
                timestampFormat = "yyyy-MM-dd HH:mm:ss";
            if (dateFormat == null)
                dateFormat = "yyyy-MM-dd";
            if (timeFormat == null)
                timeFormat = "HH:mm:ss";

            // Remove trailing back slash
            if (badRecordDirectory != null)
                badRecordDirectory = badRecordDirectory.replaceAll("/$", "");

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
                "," +
                quoteStringArgument(badRecordDirectory) +
                "," +
                badRecordsAllowed +
                " )";
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);
            List<String> insertColumnList = null;
            if (insertColumnListString != null &&
                ! insertColumnListString.isEmpty() &&
                ! insertColumnListString.toLowerCase().equals("null")) {
                insertColumnList = normalizeIdentifierList(insertColumnListString);
            }


            if (samplingOnly == null)
                samplingOnly ="false";

            if (outputKeysOnly == null)
                outputKeysOnly = "false";

            if (skipSampling == null)
                skipSampling = "false";

            LanguageConnectionContext lcc = ((EmbedConnection)conn).getLanguageConnection();

            ColumnInfo columnInfo = new ColumnInfo(conn, schemaName, tableName, insertColumnList);
            String selectList = generateColumnList(lcc,schemaName,tableName,insertColumnList, true, null);
            String vtiTable = importVTI + " AS importVTI (" + columnInfo.getImportAsColumns() + ")";
            String insertSql = "INSERT INTO " + entityName + "(" + columnInfo.getInsertColumnNames() + ") " +
                    "--splice-properties useSpark=true , insertMode=" + (isUpsert ? "UPSERT" : "INSERT") + ", statusDirectory=" +
                    badRecordDirectory + ", badRecordsAllowed=" + badRecordsAllowed + ", bulkImportDirectory=" + bulkImportDirectory
                    + ", samplingOnly=" + samplingOnly + ", outputKeysOnly=" + outputKeysOnly + ", skipSampling=" + skipSampling
                    + (skipConflictDetection ? ", skipConflictDetection=true" : "") + (skipWAL ? ", skipWAL=true" : "")
                    + (indexName !=null ? (", index=" + indexName):"") + "\n" +
                    " SELECT "+ selectList +
                    " from " + vtiTable;

            long rowsUpdated = 0;
            if (isMerge) {
                //get the PK from the target table
                String[] pkColums = getPKList(lcc, schemaName, tableName);

                //Error out if PK is not available
                if (pkColums == null)
                    throw PublicAPI.wrapStandardException(ErrorState.UPSERT_NO_PRIMARY_KEYS.newException(""+schemaName +"." + tableName +""));

                // Error out if PK does not appear in the insertColumnList
                for (String pk : pkColums) {
                    if (!columnInfo.getImportColumns().contains(pk))
                        throw PublicAPI.wrapStandardException(ErrorState.IMPORT_MISSING_NOT_NULL_KEY.newException("" + schemaName + "." + tableName + "." + pk + ""));
                }
                //compose the PK condition list
                String pkConditions = getPKConditions(schemaName, tableName, pkColums);

                // In the SET clause, even for column with default value, we don't want to use the case expression but
                // just need the column name
                // Remove the PK columns from the insertColumnList to avoid using PkRowHash encoding
                // which incurs an RPC call for every row to update
                List<String> insertColumnListExcludingPK = new ArrayList<>();
                List<String> pkList = Arrays.asList(pkColums);
                for (Object insertColumn: columnInfo.getImportColumns()) {
                    if (!pkList.contains(insertColumn))
                        insertColumnListExcludingPK.add((String)insertColumn);
                }

                // we don't need an update statement if the only columns to update are PK columns
                if (!insertColumnListExcludingPK.isEmpty()) {
                    String setColumnList = generateColumnList(lcc, schemaName, tableName, insertColumnListExcludingPK, false, null);
                    String subqSelect = generateColumnList(lcc, schemaName, tableName, insertColumnListExcludingPK, true, "importVTI");

                    String updateSql = "UPDATE " + entityName + "--splice-properties useSpark=true" +
                            "\n set (" + setColumnList + ") = \n" +
                            "(SELECT " + subqSelect + " from " + vtiTable + " " +
                            pkConditions + ")";

                    //update for rows that has match for PK
                    try (PreparedStatement ips = conn.prepareStatement(updateSql)) {
                        ips.executeUpdate();
                        rowsUpdated = ips.getUpdateCount();
                    } catch (Exception e) {
                        throw new SQLException(e);
                    }
                }

                //update the insertSQL with conditions
                insertSql += " WHERE not exists (select 1 from " + entityName + " " + pkConditions + ")";
            }

            //prepare the import statement to hit any errors before locking the table
            //execute the import operation.
            try (PreparedStatement ips = conn.prepareStatement(insertSql)) {
                FileInfo contentSummary = ImportUtils.getImportFileInfo(fileName);
                ips.executeUpdate();
                String badFileName = ((EmbedConnection) conn).getLanguageConnection().getBadFile();
                ExecRow result = new ValueRow(5);
                long rowsImported = ((EmbedConnection) conn).getLanguageConnection().getRecordsImported();
                long rowsAffected = rowsImported + rowsUpdated;
                if (isMerge) {
                    result.setRowArray(new DataValueDescriptor[]{
                            new SQLLongint(rowsAffected),
                            new SQLLongint(rowsUpdated),
                            new SQLLongint(rowsImported),
                            new SQLLongint(((EmbedConnection) conn).getLanguageConnection().getFailedRecords()),
                            new SQLLongint(contentSummary.fileCount()),
                            new SQLLongint(contentSummary.size()),
                            new SQLVarchar((badFileName == null || badFileName.isEmpty() ? "NONE" : badFileName))
                    });
                } else {
                    result.setRowArray(new DataValueDescriptor[]{
                            new SQLLongint(rowsImported),
                            new SQLLongint(((EmbedConnection) conn).getLanguageConnection().getFailedRecords()),
                            new SQLLongint(contentSummary.fileCount()),
                            new SQLLongint(contentSummary.size()),
                            new SQLVarchar((badFileName == null || badFileName.isEmpty() ? "NONE" : badFileName))
                    });
                }
                Activation act = ((EmbedConnection) conn).getLanguageConnection().getLastActivation();
                IteratorNoPutResultSet rs = isMerge ?
                    new IteratorNoPutResultSet(Collections.singletonList(result), MERGE_RESULT_COLUMNS, act)
                        :
                    new IteratorNoPutResultSet(Collections.singletonList(result),
                                               (isCheckScan ? CHECK_RESULT_COLUMNS : IMPORT_RESULT_COLUMNS), act);
                rs.open();
                results[0] = new EmbedResultSet40((EmbedConnection) conn, rs, false, null, true);
            } catch (SQLException | StandardException | IOException e) {
                throw new SQLException(e);
            }
        } finally {
            if (conn != null) {
                ((EmbedConnection) conn).getLanguageConnection().resetBadFile();
                ((EmbedConnection) conn).getLanguageConnection().resetFailedRecords();
                ((EmbedConnection) conn).getLanguageConnection().resetRecordsImported();
                conn.close();
            }
        }
    }

    private static String generateColumnList(LanguageConnectionContext lcc,
                                             String schemaName,
                                             String tableName,
                                             List<String> insertColumnList,
                                             boolean genCaseExp,
                                             String qualifierName) throws SQLException{
        DataDictionary dd = lcc.getDataDictionary();
        StringBuilder colListStr = new StringBuilder();
        try{
            SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName,lcc.getTransactionExecute(),true);
            assert sd!=null: "Programmer error: schema is not found!";
            TableDescriptor td = dd.getTableDescriptor(tableName,sd,lcc.getTransactionExecute());
            assert td!=null: "Programmer error: table is not found!";

            ColumnDescriptorList columnDescriptorList=td.getColumnDescriptorList();
            if(insertColumnList!=null){
                boolean isFirst = true;
                for(String col:insertColumnList){
                    if(isFirst) isFirst = false;
                    else colListStr = colListStr.append(",");
                    ColumnDescriptor cd = columnDescriptorList.getColumnDescriptor(td.getUUID(),col);
                    if(cd==null)
                        throw StandardException.newException(SQLState.COLUMN_NOT_FOUND,tableName+"."+col); //shouldn't happen, but just in case
                    colListStr = writeColumn(cd,colListStr,genCaseExp, qualifierName);
                }
            }else{
                boolean isFirst = true;
                for(ColumnDescriptor cd: columnDescriptorList){
                    if(isFirst) isFirst = false;
                    else colListStr = colListStr.append(",");
                    colListStr = writeColumn(cd,colListStr,genCaseExp, qualifierName);
                }
            }

            return colListStr.toString();
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static StringBuilder writeColumn(ColumnDescriptor cd,StringBuilder text, boolean genCaseExpr, String qualifierName) throws StandardException{
        String colName = sqlFormat(cd.getColumnName());
        DefaultInfo di = cd.getDefaultInfo();
        if (qualifierName != null)
            colName = qualifierName + "." + colName;
        if(di!=null && genCaseExpr){
            text = text.append("CASE WHEN (")
                    .append(colName)
                    .append(" IS NULL) THEN ")
                    .append(di.getDefaultText())
                    .append(" ELSE ")
                    .append(colName)
                    .append(" END");
        }else text = text.append(colName);
        return text;
    }

    private static String sqlFormat(String columnName){
//        if(columnName.toUpperCase().equals(columnName) || columnName.toLowerCase().equals(columnName))
//            return columnName;
        return "\""+columnName+"\"";
    }

    static List<String> normalizeIdentifierList(String insertColumnListStr) {
        List<String> normalizedList = new ArrayList<>();

        String[] candidateColumns = insertColumnListStr.split(",");
        List<String> columns = getColumns(candidateColumns);
        for (String columnName : columns) {
            String normalizedColumnName = normalize(columnName);
            normalizedList.add(normalizedColumnName);
        }

        return normalizedList;
    }

    static List<String> getColumns(String[] s) {
        List<String> columns = Lists.newArrayList();
        int i = 0;
        boolean closed = true;
        while(i < s.length) {
            if (!s[i].startsWith("\"")) {
                columns.add(s[i]);
            }
            else {
                closed = false;
                StringBuffer col = new StringBuffer(s[i]);
                while(!s[i].endsWith("\"") && i < s.length) {
                    ++i;
                    col.append(",").append(s[i]);
                }
                closed = col.toString().endsWith("\"");
                if (closed){
                    columns.add(col.toString());
                }
                else {
                    throw new RuntimeException(String.format("Column name %s not well-formatted", col));
                }
            }
            ++i;
        }
        return columns;
    }
    static String normalize(String s) {
        s = s.trim();
        if (!s.startsWith("\"") && !s.endsWith("\"")) {
            return s.toUpperCase();
        }
        else if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length()-1);
        }
        else {
            throw new RuntimeException(String.format("Column name %s not well-formatted", s));
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


    private static String[] getPKList(LanguageConnectionContext lcc,
                                   String schemaName,
                                   String tableName) throws SQLException {
        DataDictionary dd = lcc.getDataDictionary();
        String[] pkNames = null;

        try {
            SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, lcc.getTransactionExecute(), true);
            TableDescriptor td = dd.getTableDescriptor(tableName, sd, lcc.getTransactionExecute());
            ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();

            int[] pkList = null;
            for(int i=0;i<cdl.size();i++){
                ConglomerateDescriptor cd = cdl.get(i);
                if(cd.isPrimaryKey()){
                     pkList = cd.getIndexDescriptor().baseColumnPositions();
                     break;
                }
            }

            if (pkList != null) {
                ColumnDescriptorList columnDescriptorList=td.getColumnDescriptorList();
                pkNames = new String[pkList.length];
                for (int i=0; i< pkList.length; i++) {
                    pkNames[i] = columnDescriptorList.getColumnDescriptor(td.getUUID(),pkList[i]).getColumnName();
                }
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
        return pkNames;
    }

    private static String getPKConditions(String schemaName, String tableName, String[] pkColumns) {
        StringBuffer sb = new StringBuffer();
        sb.append("WHERE ");
        boolean first = true;
        for (int i=0; i<pkColumns.length; i++) {
            if (!first)
                sb.append(" AND ");
            else
                first = false;
            sb.append(IdUtil.normalToDelimited(schemaName));
            sb.append(".");
            sb.append(IdUtil.normalToDelimited(tableName));
            sb.append(".");
            sb.append(IdUtil.normalToDelimited(pkColumns[i]));
            sb.append(" = importVTI.");
            sb.append(IdUtil.normalToDelimited(pkColumns[i]));
        }
        return sb.toString();
    }
}
