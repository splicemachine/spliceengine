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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.pipeline.ErrorState;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;


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


    private static final String TABLEID_FROM_SCHEMA = "select tableid from sys.systables t where t.schemaid = ?";
    public static List<TableDescriptor> getAllTableDescriptors(SchemaDescriptor sd,EmbedConnection conn) throws
            SQLException {
        try (PreparedStatement statement = conn.prepareStatement(TABLEID_FROM_SCHEMA)) {
            statement.setString(1, sd.getUUID().toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
                UUIDFactory uuidFactory = dd.getUUIDFactory();
                List<TableDescriptor> tds = new LinkedList<>();
                while (resultSet.next()) {
                    com.splicemachine.db.catalog.UUID tableId = uuidFactory.recreateUUID(resultSet.getString(1));
                    TableDescriptor tableDescriptor = dd.getTableDescriptor(tableId);
                    /*
                     * We need to filter out views from the TableDescriptor list. Views
                     * are special cases where the number of conglomerate descriptors is 0. We
                     * don't collect statistics for those views
                     */

                    if (tableDescriptor != null && tableDescriptor.getConglomerateDescriptorList().size() > 0) {
                        tds.add(tableDescriptor);
                    }
                }
                return tds;
            }
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }
}
