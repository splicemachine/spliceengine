package com.splicemachine.derby.impl.sql.execute.sequence;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

public class SpliceSequenceKey extends AbstractSequenceKey {	
    private final long seqConglomId;
    private final int columnNum;
    private boolean systemTableSearched = false;
    private final DataDictionary metaDictionary;
    protected HTableInterface table;

    public SpliceSequenceKey(HTableInterface table,
    		byte[] sysColumnsRow,
            long seqConglomId,
            int columnNum,
            DataDictionary metaDictionary,
            long blockAllocationSize) {
    	super(sysColumnsRow, blockAllocationSize);
    	this.seqConglomId = seqConglomId;
    	this.columnNum = columnNum;
    	this.metaDictionary = metaDictionary;
    	this.table = table;
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
	
	public SpliceSequence makeNew() {
        return new SpliceSequence(table,
                blockAllocationSize,sysColumnsRow,
                autoIncStart,autoIncrement);		
	}
	
}
