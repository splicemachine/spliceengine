package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.pipeline.ErrorState;

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
        else
            schema = schema.toUpperCase();
        return schema;
    }

    public static String validateTable(String table) throws SQLException {
        if (table == null)
            throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
        else
            table = table.toUpperCase();
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


    public static void resultValuesToNull(DataValueDescriptor[] dvds) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd != null){
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_DOUBLE_ID:
                        dvd.restoreToNull();
                        break;
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                        dvd.restoreToNull();
                        break;
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        dvd.restoreToNull();
                        break;
                    case StoredFormatIds.SQL_LONGINT_ID:
                        dvd.restoreToNull();
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
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.setValue(defaultValue);
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }
}
