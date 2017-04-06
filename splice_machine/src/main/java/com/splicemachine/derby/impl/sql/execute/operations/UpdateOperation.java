/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.actions.UpdateConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.function.InsertPairFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jessiezhang
 * @author Scott Fines
 */
public class UpdateOperation extends DMLWriteOperation{
    private static final Logger LOG=Logger.getLogger(UpdateOperation.class);
    private DataValueDescriptor[] kdvds;
    public int[] colPositionMap;
    public FormatableBitSet heapList; // 1-based
    public FormatableBitSet heapListStorage; // 1-based
    protected int[] columnOrdering;
    protected int[] format_ids;

    protected static final String NAME=UpdateOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName(){
        return NAME;
    }

    @SuppressWarnings("UnusedDeclaration")
    public UpdateOperation(){
        super();
    }

    int[] pkCols;
    FormatableBitSet pkColumns;

    public UpdateOperation(SpliceOperation source,GeneratedMethod generationClauses,
                           GeneratedMethod checkGM,Activation activation,double optimizerEstimatedRowCount,
                           double optimizerEstimatedCost,String tableVersion)
            throws StandardException, IOException{
        super(source,generationClauses,checkGM,activation,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        init();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"init");
        super.init(context);
        heapConglom=writeInfo.getConglomerateId();
        pkCols=writeInfo.getPkColumnMap();
        pkColumns=writeInfo.getPkColumns();
        SpliceConglomerate conglomerate=(SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(heapConglom);
        format_ids=conglomerate.getFormat_ids();
        columnOrdering=conglomerate.getColumnOrdering();
        kdvds=new DataValueDescriptor[columnOrdering.length];
        for(int i=0;i<columnOrdering.length;++i){
            kdvds[i]=LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
        }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getColumnPositionMap(FormatableBitSet heapList){
        if(colPositionMap==null){
                /*
			       * heapList is the position of the columns in the original row (e.g. if cols 2 and 3 are being modified,
						 * then heapList = {2,3}). We have to take that position and convert it into the actual positions
						 * in nextRow.
						 *
						 * nextRow looks like {old,old,...,old,new,new,...,new,rowLocation}, so suppose that we have
						 * heapList = {2,3}. Then nextRow = {old2,old3,new2,new3,rowLocation}. Which makes our colPositionMap
						 * look like
						 *
						 * colPositionMap[2] = 2;
						 * colPositionMap[3] = 3;
						 *
						 * But if heapList = {2}, then nextRow looks like {old2,new2,rowLocation}, which makes our colPositionMap
						 * look like
						 *
						 * colPositionMap[2] = 1
						 *
						 * in general, then
						 *
						 * colPositionMap[i= heapList.anySetBit()] = nextRow[heapList.numSetBits()]
						 * colPositionMap[heapList.anySetBit(i)] = nextRow[heapList.numSetBits()+1]
						 * ...
						 *
						 * and so forth
						 */
            colPositionMap=new int[heapList.size()];

            for(int i=heapList.anySetBit(), pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++){
                colPositionMap[i]=pos;
            }
        }
        return colPositionMap;
    }

    public FormatableBitSet getHeapList() throws StandardException{
        if(heapList==null){
            heapList=((UpdateConstantOperation)writeInfo.getConstantAction()).getBaseRowReadList();
            if(heapList==null){
                ExecRow row=((UpdateConstantOperation)writeInfo.getConstantAction()).getEmptyHeapRow(activation.getLanguageConnectionContext());
                int length=row.getRowArray().length;
                heapList=new FormatableBitSet(length+1);

                for(int i=1;i<length+1;++i){
                    heapList.set(i);
                }
            }
        }
        return heapList;
    }

    private FormatableBitSet getHeapListStorage() throws StandardException{
        // Creates the equivalent of heapList (which is based on ordinal column positions)
        // but where the bits represent storage positions of columns being updated.
        if (heapListStorage==null) {
            FormatableBitSet heapList = getHeapList();
            int[] storagePositionIds = ((UpdateConstantOperation)writeInfo.getConstantAction()).getStoragePositionIds();
            heapListStorage = new FormatableBitSet(heapList.getLength());
            for(int i=heapList.anySetBit();i>=0;i=heapList.anySetBit(i)) {
                int storagePos = storagePositionIds[i-1]+1;
                heapListStorage.grow(storagePos+1);
                heapListStorage.set(storagePos);
            }
        }
        return heapListStorage;
    }

    @Override
    public String toString(){
        return "Update{destTable="+heapConglom+",source="+source+"}";
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        DataSet set=source.getDataSet(dsp);
        OperationContext operationContext=dsp.createOperationContext(this);
        TxnView txn=getCurrentTransaction();
        ExecRow execRow=getExecRowDefinition();
        int[] execRowTypeFormatIds=WriteReadUtils.getExecRowTypeFormatIds(execRow);
        operationContext.pushScope();
        try{
            PairDataSet toWrite=set.index(new InsertPairFunction(operationContext),true);
            DataSetWriter writer=toWrite.updateData(operationContext)
                    .execRowDefinition(execRow)
                    .execRowTypeFormatIds(execRowTypeFormatIds)
                    .pkCols(pkCols==null?new int[0]:pkCols)
                    .pkColumns(pkColumns)
                    .formatIds(format_ids)
                    .columnOrdering(columnOrdering==null?new int[0]:columnOrdering)
                    .heapList(getHeapListStorage())
                    .tableVersion(tableVersion)
                    .destConglomerate(heapConglom)
                    .operationContext(operationContext)
                    .txn(txn)
                    .build();
            return writer.write();
        }finally{
            operationContext.popScope();
        }
    }
}
