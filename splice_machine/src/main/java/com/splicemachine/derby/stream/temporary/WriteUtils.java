package com.splicemachine.derby.stream.temporary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;

/**
 * Created by jleach on 5/6/15.
 */
public class WriteUtils {

    public void getStartAndIncrementFromSystemTables(DataDictionary metaDictionary, int columnNum, long seqConglomId, long autoIncStart, long autoIncrement) throws StandardException {
        ConglomerateDescriptor conglomerateDescriptor = metaDictionary.getConglomerateDescriptor(seqConglomId);
        TableDescriptor tableDescriptor = metaDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
        ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
        for(Object o:columnDescriptorList){
            ColumnDescriptor cd = (ColumnDescriptor)o;
            if(cd.getPosition()==columnNum){
                autoIncStart = cd.getAutoincStart();
                autoIncrement = cd.getAutoincInc();
                break;
            }
        }
    }

    /*	@Override
	protected void getStartAndIncrementFromSystemTables() throws StandardException {
        if(systemTableSearched) return;
        ConglomerateDescriptor conglomerateDescriptor = metaDictionary.getConglomerateDescriptor(seqConglomId);
        TableDescriptor tableDescriptor = metaDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
        ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
        for(Object o:columnDescriptorList){
            ColumnDescriptor cd = (ColumnDescriptor)o;
            if(cd.getPosition()==columnNum){
                autoIncStart = cd.getAutoincStart();
                autoIncrement = cd.getAutoincInc();
                break;
            }
        }
        systemTableSearched = true;
    }
*/

}
