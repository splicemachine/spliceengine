/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedDatabaseMetaData;
import com.splicemachine.pipeline.ErrorState;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public class EngineUtils{
      public static int[] bitSetToMap(FormatableBitSet bitSet){
        if(bitSet==null) return null;
        int[] validCols = new int[bitSet.getNumBitsSet()];
        int pos=0;
        for(int i=bitSet.anySetBit();i!=-1;i=bitSet.anySetBit(i)){
            validCols[pos] = i;
            pos++;
        }
        return validCols;
    }

    public static int[] getFormatIds(DataValueDescriptor[] dvds) {
        int[] ids = new int [dvds.length];
        for (int i = 0; i < dvds.length; ++i) {
            ids[i] = dvds[i].getTypeFormatId();
        }
        return ids;
    }

    public static String validateSchema(String schema) throws SQLException{
        if (schema == null)
            schema = getCurrentSchema();
        else{
            schema = schema.trim();
            int quoteStartIdx = schema.indexOf("\"");
            if(quoteStartIdx>=0){
                int quoteEndIdx = schema.indexOf("\"",quoteStartIdx+1);
                assert quoteEndIdx>0 : "Parser error! uncompleted quotes were allowed!";
                schema = schema.substring(quoteStartIdx+1,quoteEndIdx);
            }else
                schema = schema.toUpperCase();
        }
        return schema;
    }

    public static String validateTable(String table) throws SQLException {
        if (table == null)
            throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
        else {
            table = table.trim();
            int quoteStartIdx = table.indexOf("\"");
            if(quoteStartIdx>=0){
                int quoteEndIdx = table.indexOf("\"",quoteStartIdx+1);
                assert quoteEndIdx>0 : "Parser error! uncompleted quotes were allowed!";
                table = table.substring(quoteStartIdx+1,quoteEndIdx);
            }else
                table = table.toUpperCase();
        }
        return table;
    }

    public static String validateColumnName(String columnName) throws SQLException {
        if (columnName == null)
            throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_ID.newException());
        else
            columnName = columnName.toUpperCase();
        return columnName;
    }

    public static String getCurrentSchema() throws SQLException {
        EmbedConnection connection = (EmbedConnection) SpliceAdmin.getDefaultConn();
        assert connection != null;
        LanguageConnectionContext lcc = connection.getLanguageConnection();
        assert lcc != null;
        return lcc.getCurrentSchemaName();
    }


    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    public static void resultValuesToNull(DataValueDescriptor[] dvds) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd != null){
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_DOUBLE_ID:
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                    case StoredFormatIds.SQL_LONGINT_ID:
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.restoreToNull();
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }

    /**
     * Populates an array of DataValueDescriptors with a default value based on their type.
     *
     * This is used mainly to prevent NullPointerExceptions from occurring in administrative
     * operations such as getExecRowDefinition().
     *
     * @param dvds the descriptors to populate
     * @param defaultValue the value to default each descriptor to
     *
     * @throws StandardException
     */
    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
    public static void populateDefaultValues(DataValueDescriptor[] dvds,int defaultValue) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd != null){
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_DOUBLE_ID:
                        dvd.setValue((double)defaultValue); //set to one to prevent /-by-zero errors
                        break;
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                        dvd.setValue(defaultValue);
                        break;
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        dvd.setValue(false);
                        break;
                    case StoredFormatIds.SQL_LONGINT_ID:
                        dvd.setValue(defaultValue);
                        break;
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.setValue(defaultValue);
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }

    public static void checkSchemaVisibility(String schemaName) throws SQLException, StandardException {
        EmbedConnection conn = (EmbedConnection) SpliceAdmin.getDefaultConn();
        EmbedDatabaseMetaData dmd = (EmbedDatabaseMetaData)conn.getMetaData();
        try (ResultSet rs = dmd.getSchemas(null,schemaName)) {
            if (!rs.next()) {
                throw StandardException.newException(SQLState.LANG_SCHEMA_DOES_NOT_EXIST, schemaName);
            }
        }
    }

    public static TableDescriptor verifyTableExists(Connection conn, String schema, String table) throws
            SQLException, StandardException {
        LanguageConnectionContext lcc = ((EmbedConnection) conn).getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        // check schema visiblity to the current user
        checkSchemaVisibility(schema);
        SchemaDescriptor schemaDescriptor = getSchemaDescriptor(schema, lcc, dd);
        TableDescriptor tableDescriptor = dd.getTableDescriptor(table, schemaDescriptor, lcc.getTransactionExecute());
        if (tableDescriptor == null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema + "." + table);

        return tableDescriptor;
    }

    public static SchemaDescriptor getSchemaDescriptor(String schema,
                                                        LanguageConnectionContext lcc,
                                                        DataDictionary dd) throws StandardException {
        SchemaDescriptor schemaDescriptor;
        if (schema ==null || (schemaDescriptor = dd.getSchemaDescriptor(schema, lcc.getTransactionExecute(), true))==null)
            throw ErrorState.LANG_SCHEMA_DOES_NOT_EXIST.newException(schema);
        return schemaDescriptor;
    }
}
