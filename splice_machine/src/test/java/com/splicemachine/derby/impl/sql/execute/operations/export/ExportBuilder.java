/*
 * Copyright (c) 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportBuilder {
    String selectQuery;
    String path;
    String compression;
    String replicationCount;
    String encoding;
    String fieldSeparator; // = columnDelimiter. by default ','
    String quoteCharacter; // = characterDelimiter
    String quoteMode;
    String floatingPointNotation;
    String timestampFormat;
    String format = "csv";
    String schemaName;
    boolean useNativeSyntax;
    boolean useKeywords;

    SpliceWatcher methodWatcher;

    public ExportBuilder() {
        this(false, false, null, null, null);
    }

    public ExportBuilder(boolean useNativeSyntax, boolean useKeywords,
                         String schemaName, SpliceWatcher methodWatcher, String selectQuery) {
        this.methodWatcher = methodWatcher;
        this.selectQuery = selectQuery;
        this.schemaName = schemaName;
        this.useNativeSyntax = useNativeSyntax;
        this.useKeywords = useKeywords;
    }
    public ExportBuilder path(String val) { path = val; return this; }
    public ExportBuilder compression(String val) { compression = val; return this; }
    public ExportBuilder replicationCount(String val) { replicationCount = val; return this; }
    public ExportBuilder encoding(String val) { encoding = val; return this; }
    public ExportBuilder fieldSeparator(String val) { fieldSeparator = val; return this; }
    public ExportBuilder quoteCharacter(String val) { quoteCharacter = val; return this; }
    public ExportBuilder quoteMode(String val) { quoteMode = val; return this; }
    public ExportBuilder floatingPointNotation(String val) { floatingPointNotation = val; return this; }
    public ExportBuilder timestampFormat(String val) { timestampFormat = val; return this; }
    public ExportBuilder format(String val) { format = val; return this; }



    String strOrNull(String s) {
        if( s == null)
            return "null, ";
        else
            return "'" + s + "', ";
    }

    public String externalTableCsvOptions() {
        /*
         [ ROW FORMAT DELIMITED
         [ FIELDS TERMINATED BY char [ESCAPED BY char] ]
         [ LINES TERMINATED BY char ]
         */
        if(fieldSeparator == null) return "";
        else return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fieldSeparator + "'";

    }

    public String externalTextfile(String table, String tableSchema) {
        return "CREATE EXTERNAL TABLE " + table + " (" + tableSchema + ") " + externalTableCsvOptions()
                + "stored as textfile location '" + path + "'";
    }

    public String importSql(String function, String schema, String table) {
        return "call SYSCS_UTIL." + function + "(" + strOrNull(schema) + strOrNull(table) +
                "null, " + //  insertColumnList
                strOrNull(path) + // path
                strOrNull(fieldSeparator) + // columnDelimiter
                strOrNull(quoteCharacter) + // characterDelimiter
                strOrNull( timestampFormat) +
                strOrNull(null) + // dateFormat
                strOrNull( null) + // timeFormat
                "'0', " + // badRecordsAllowed
                "'/tmp', " + // badRecordLogDirectory
                "false, " + // oneLineRecords
                "null" + // charset
                ")";
    }

    public String selectSpliceFileVTI(String columns, String output) {
        return "select " + columns + " from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                strOrNull(path) + // path
                strOrNull(quoteCharacter) + // characterDelimiter
                strOrNull(fieldSeparator) + // columnDelimiter
                "null, " + // columnIndex
                strOrNull( null) + // timeFormat
                strOrNull( timestampFormat) +
                strOrNull(null) + // dateFormat
                "'false', " + // oneLineRecords
                "'UTF-8'" + // charset
                ") AS MYDATA (" + output + ") --splice-properties useOLAP=true";
    }

    void testImportExport(String tableName, String columns, long expectedExportRowCount) throws Exception {
        testImportExport(false, tableName, columns, expectedExportRowCount);
    }

    void testImportExport(boolean loadReplace, String tableName, String columns,
                          long expectedExportRowCount) throws Exception {

        exportAndAssertExportResults(methodWatcher,
                exportSql(), expectedExportRowCount);

        String query = "select " + columns + " from " + tableName;
        String res1 = methodWatcher.executeToString(query, true);

        String SQL = importSql( "IMPORT_DATA", schemaName, tableName);
        System.out.println(SQL);
//        methodWatcher.execute("DELETE FROM " + tableName);
//        methodWatcher.execute(importSql( "IMPORT_DATA", schemaName, tableName));

        String res2 = methodWatcher.executeToString(query, true);
        Assert.assertEquals(res1, res2);

        if(loadReplace) {
            methodWatcher.execute(importSql( "LOAD_REPLACE", schemaName, tableName));
            String res3 = methodWatcher.executeToString(query, true);
            Assert.assertEquals(res1, res3);
        }
    }


    public String exportSql() {
        if (useNativeSyntax) {
            StringBuilder sql = new StringBuilder();
            sql.append("EXPORT TO '").append(path).append("'");
            if (useKeywords)
                sql.append(" AS " + format.toUpperCase() + " ");
            else
                sql.append(" AS '" + format + "' ");
            if (compression != null) {
                sql.append(" COMPRESSION ");
                if (useKeywords)
                    sql.append(compression);
                else
                    sql.append("'").append(compression).append("'");
            }
            if (replicationCount != null) {
                sql.append(" REPLICATION_COUNT ").append(replicationCount);
            }
            if (encoding != null) {
                sql.append(" ENCODING '").append(encoding).append("'");
            }
            if (fieldSeparator != null) {
                sql.append(" FIELD_SEPARATOR '").append(fieldSeparator).append("'");
            }
            if (quoteCharacter != null) {
                sql.append(" QUOTE_CHARACTER '").append(quoteCharacter).append("'");
            }
            if (quoteMode != null) {
                sql.append(" QUOTE_MODE ");
                if (useKeywords)
                    sql.append(quoteMode);
                else
                    sql.append("'").append(quoteMode).append("'");
            }
            if (floatingPointNotation != null) {
                sql.append(" FLOATING_POINT_NOTATION ");
                if (useKeywords)
                    sql.append(floatingPointNotation);
                else
                    sql.append("'").append(floatingPointNotation).append("'");
            }
            if (timestampFormat != null) {
                sql.append(" TIMESTAMP_FORMAT '").append(timestampFormat).append("'");
            }
            sql.append(" ").append(selectQuery);
            return sql.toString();
        } else {
            StringBuilder sql = new StringBuilder();
            sql.append("EXPORT('").append(path).append("', ");
            if (compression == null) {
                sql.append("null, ");
            } else if (compression.toUpperCase().equals("TRUE") || compression.toLowerCase().equals("FALSE")) {
                sql.append(compression.toLowerCase()).append(", ");
            } else {
                sql.append("'").append(compression).append("', ");
            }
            if (replicationCount == null) {
                sql.append("null, ");
            } else {
                sql.append(replicationCount).append(", ");
            }
            if (encoding == null) {
                sql.append("null, ");
            } else {
                sql.append("'").append(encoding).append("', ");
            }
            if (fieldSeparator == null) {
                sql.append("null, ");
            } else {
                sql.append("'").append(fieldSeparator).append("', ");
            }
            if (quoteCharacter == null) {
                sql.append("null)");
            } else {
                sql.append("'").append(quoteCharacter).append("')");
            }
            sql.append(" ").append(selectQuery);
            assert quoteMode == null;
            assert floatingPointNotation == null;
            assert timestampFormat == null;
            return sql.toString();
        }
    }
    public static void exportAndAssertExportResults(
            SpliceWatcher methodWatcher, String exportSQL, long expectedExportRowCount) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(exportSQL);
        assertTrue(resultSet.next());
        long exportedRowCount = resultSet.getLong(1);
//        long exportTimeMs = resultSet.getLong(2);
        assertEquals(expectedExportRowCount, exportedRowCount);
//        assertTrue(exportTimeMs >= 0);
    }
}
