package com.splicemachine.derby.stream.temporary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.utils.Pair;

/**
 * Created by jleach on 5/6/15.
 */
public class WriteReadUtils {
    /*
    public static void getStartAndIncrementFromSystemTables(DataDictionary metaDictionary, int columnNum, long seqConglomId,
                                                            long autoIncStart, long autoIncrement) throws StandardException {
        ConglomerateDescriptor conglomerateDescriptor = metaDictionary.getConglomerateDescriptor(seqConglomId);
        TableDescriptor tableDescriptor = metaDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
        ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
        for (Object o : columnDescriptorList) {
            ColumnDescriptor cd = (ColumnDescriptor) o;
            if (cd.getPosition() == columnNum) {
                autoIncStart = cd.getAutoincStart();
                autoIncrement = cd.getAutoincInc();
                break;
            }
        }
    }
    */

    public static int[] getExecRowTypeFormatIds(ExecRow currentTemplate) throws StandardException {
       int[] execRowTypeFormatIds = new int[currentTemplate.nColumns()];
        for(int i = 0;i<currentTemplate.nColumns();i++) {
            execRowTypeFormatIds[i] = currentTemplate.getColumn(i + 1).getTypeFormatId();
        }
        return execRowTypeFormatIds;
    }

	public static Pair<Long,Long>[] getStartAndIncrementFromSystemTables(RowLocation[] autoIncrementRowLocationArray,DataDictionary dataDictionary, long seqConglomId) throws StandardException {
        Pair<Long,Long>[] defaultAutoIncrementValues = new Pair[autoIncrementRowLocationArray.length];
        ConglomerateDescriptor conglomerateDescriptor = dataDictionary.getConglomerateDescriptor(seqConglomId);
        TableDescriptor tableDescriptor = dataDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
        ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
        for(int i = 0; i< autoIncrementRowLocationArray.length; i++){
            ColumnDescriptor cd = columnDescriptorList.get(i);
            defaultAutoIncrementValues[i] = new Pair<>(cd.getAutoincStart(),cd.getAutoincInc());
        }
        return defaultAutoIncrementValues;
    }

}