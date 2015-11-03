package com.splicemachine.derby.impl.load;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import com.splicemachine.db.iapi.jdbc.EngineConnection;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.load.ColumnInfo;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.pipeline.exception.ErrorState;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

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
		private HBaseAdmin admin;

		private static final ResultColumnDescriptor[] IMPORT_RESULT_COLUMNS = new GenericColumnDescriptor[]{
                new GenericColumnDescriptor("rowsImported",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("failedRows",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("files",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
                new GenericColumnDescriptor("dataSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("failedLog",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
		};

		private static final ResultColumnDescriptor[] CHECK_RESULT_COLUMNS = new GenericColumnDescriptor[]{
                new GenericColumnDescriptor("rowsChecked",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("failedRows",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("files",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
                new GenericColumnDescriptor("dataSize",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
                new GenericColumnDescriptor("failedLog",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
		};

    public static void UPSERT_DATA_FROM_FILE(String schemaName, String tableName,
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
    ) throws SQLException{
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

    public static void UPSERT_CHECK_DATA_FROM_FILE(String schemaName, String tableName,
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
                true,
                results);
    }

	public static void IMPORT_DATA(String schemaName, String tableName,
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

	public static void IMPORT_CHECK_DATA(String schemaName, String tableName,
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
            true,
            results);
	}

    private static void doImport(String schemaName, String tableName,
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
            SpliceLogUtils.trace(LOG,"doImport {schemaName=%s, tableName=%s, insertColumnList=%s, fileName=%s, " +
            "columnDelimiter=%s, characterDelimiter=%s, timestampFormat=%s, dateFormat=%s, timeFormat=%s, " +
            "badRecordsAllowed=%d, badRecordDirectory=%s, oneLineRecords=%s, charset=%s, isUpsert=%s, isCheckScan=%s",
                    schemaName,tableName,insertColumnList,fileName,columnDelimiter,characterDelimiter,
                    timestampFormat,dateFormat,timeFormat,badRecordsAllowed,badRecordDirectory, oneLineRecords, charset, isUpsert,isCheckScan);

        if (charset ==null) {
            charset = StandardCharsets.UTF_8.name();
            System.out.println("charset->" + charset);
        }

        Connection conn = SpliceAdmin.getDefaultConn();
        try {
            assert conn != null;
            schemaName = schemaName == null?
                    ((EngineConnection) conn).getCurrentSchemaName().toUpperCase():
                    schemaName.toUpperCase();
            Activation act = ((EmbedConnection) conn).getLanguageConnection().getLastActivation();
            if (tableName == null)
                throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
            tableName = tableName.toUpperCase();
            // This needs to be found by the database locale, not hard coded.
            if (timestampFormat == null)
                timestampFormat = "yyyy-MM-dd HH:mm:ss";
            if (dateFormat == null)
                dateFormat = "yyyy-MM-dd";
            if (timeFormat == null)
                timeFormat = "HH:mm:ss";
            StringBuffer sb = new StringBuffer("new ");
            sb.append("com.splicemachine.derby.vti.SpliceFileVTI");
            sb.append("(");
            sb.append(quoteStringArgument(fileName));
            sb.append(",");
            sb.append(quoteStringArgument(characterDelimiter));
            sb.append(",");
            sb.append(quoteStringArgument(columnDelimiter));
            sb.append(",");
            sb.append(quoteStringArgument(null));
            sb.append(",");
            sb.append(quoteStringArgument(timeFormat));
            sb.append(",");
            sb.append(quoteStringArgument(dateFormat));
            sb.append(",");
            sb.append(quoteStringArgument(timestampFormat));
            sb.append(",");
            sb.append(quoteStringArgument(oneLineRecords.toLowerCase()));
            sb.append(",");
            sb.append(quoteStringArgument(charset));
            sb.append(" )");


            String importvti = sb.toString();
            String entityName = IdUtil.mkQualifiedName(schemaName, tableName);
            if (insertColumnList!= null && insertColumnList.toLowerCase().equals("null"))
                insertColumnList = null;

            ColumnInfo columnInfo = new ColumnInfo(conn, schemaName, tableName, insertColumnList!=null?insertColumnList.toUpperCase():insertColumnList);
            String insertSql = "INSERT INTO " + entityName + "("+columnInfo.getInsertColumnNames() + ") --splice-properties insertMode="+(isUpsert?"UPSERT":"INSERT")+", statusDirectory=" + badRecordDirectory + ", badRecordsAllowed=" + badRecordsAllowed + "\n"+
                    " SELECT * from " +
                    importvti + " AS importvti (" + columnInfo.getImportAsColumns() + ")";

            //prepare the import statement to hit any errors before locking the table
            PreparedStatement ips = conn.prepareStatement(insertSql);
            //execute the import operaton.
            try {
                ContentSummary contentSummary = ImportUtils.getImportDataSize(new Path(fileName));
                int count = ips.executeUpdate();
                ExecRow result=new ValueRow(5);
                result.setRowArray(new DataValueDescriptor[]{
                        new SQLLongint(count),
                        new SQLLongint(((EmbedConnection) conn).getLanguageConnection().getFailedRecords()),
                        new SQLLongint(contentSummary.getFileCount()),
                        new SQLLongint(contentSummary.getSpaceConsumed()),
                        new SQLVarchar("badFile"),
                        new SQLVarchar(((EmbedConnection) conn).getLanguageConnection().getBadFile())
                });
                IteratorNoPutResultSet rs =
                        new IteratorNoPutResultSet(Arrays.asList(result),
                                (isCheckScan?CHECK_RESULT_COLUMNS:IMPORT_RESULT_COLUMNS),act);
                rs.open();
                results[0]=new EmbedResultSet40((EmbedConnection) conn,rs,false,null,true);
            } catch (Exception e){
                throw new SQLException(e);
            }
            finally {
                ips.close();
            }
        } finally {
            if (conn!=null)
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
