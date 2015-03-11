package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceIdentityColumnKey extends AbstractSequenceKey {
    private final long seqConglomId;
    private final int columnNum;
    private boolean systemTableSearched = false;
    private final DataDictionary metaDictionary;


    public SpliceIdentityColumnKey(HTableInterface table,
    		byte[] sysColumnsRow,
            long seqConglomId,
            int columnNum,
            DataDictionary metaDictionary,
            long blockAllocationSize) {
    	super(table, sysColumnsRow, blockAllocationSize);
    	this.seqConglomId = seqConglomId;
    	this.columnNum = columnNum;
    	this.metaDictionary = metaDictionary;
    }

	@Override
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

    @Override
	public SpliceSequence makeNew() throws StandardException {
        return new SpliceSequence(table,
                blockAllocationSize,sysColumnsRow,
                getStartingValue(),getIncrementSize());		
	}
	
}
