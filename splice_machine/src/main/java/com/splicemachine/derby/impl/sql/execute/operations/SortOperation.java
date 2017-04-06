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

import org.spark_project.guava.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.stream.function.RowComparator;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SortOperation extends SpliceBaseOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(SortOperation.class);
    protected SpliceOperation source;
    protected boolean distinct;
    protected int orderingItem;
    protected int[] keyColumns;
    protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
    protected boolean[] nullsOrderedLow;
    private int numColumns;
    private ExecRow execRowDefinition=null;
    private Properties sortProperties=new Properties();
    protected static final String NAME=SortOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName(){
        return NAME;
    }

    /*
     * Used for serialization. DO NOT USE
     */
    @Deprecated
    public SortOperation(){
//		SpliceLogUtils.trace(LOG, "instantiated without parameters");
    }

    public SortOperation(SpliceOperation s,
                         boolean distinct,
                         int orderingItem,
                         int numColumns,
                         Activation a,
                         GeneratedMethod ra,
                         int resultSetNumber,
                         double optimizerEstimatedRowCount,
                         double optimizerEstimatedCost) throws StandardException{
        super(a,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.source=s;
        this.distinct=distinct;
        this.orderingItem=orderingItem;
        this.numColumns=numColumns;
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        source=(SpliceOperation)in.readObject();
        distinct=in.readBoolean();
        orderingItem=in.readInt();
        numColumns=in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeObject(source);
        out.writeBoolean(distinct);
        out.writeInt(orderingItem);
        out.writeInt(numColumns);
    }

    @Override
    public List<SpliceOperation> getSubOperations(){
        List<SpliceOperation> ops=new ArrayList<SpliceOperation>();
        ops.add(source);
        return ops;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        source.init(context);

        FormatableArrayHolder fah=(FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(orderingItem);
        if(fah==null){
            LOG.error("Unable to find column ordering for sorting!");
            throw new RuntimeException("Unable to find Column ordering for sorting!");
        }
        ColumnOrdering[] order=(ColumnOrdering[])fah.getArray(ColumnOrdering.class);

        keyColumns=new int[order.length];
        descColumns=new boolean[order.length];
        nullsOrderedLow = new boolean[order.length];
        for(int i=0;i<order.length;i++){
            keyColumns[i]=order[i].getColumnId();
            descColumns[i]=!order[i].getIsAscending();
            nullsOrderedLow[i]=order[i].getIsNullsOrderedLow();
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"keyColumns %s, distinct %s",Arrays.toString(keyColumns),distinct);
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return this.source;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException{
        if(execRowDefinition==null){
            execRowDefinition=source.getExecRowDefinition();
        }
        return execRowDefinition;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException{
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public String toString(){
        return "SortOperation {resultSetNumber="+resultSetNumber+",source="+source+"}";
    }

    public SpliceOperation getSource(){
        return this.source;
    }

    public boolean needsDistinct(){
        return this.distinct;
    }

    public Properties getSortProperties(){
        if(sortProperties==null)
            sortProperties=new Properties();

        sortProperties.setProperty("numRowsInput",""+0);
        sortProperties.setProperty("numRowsOutput",""+0);
        return sortProperties;
    }


    @Override
    public String prettyPrint(int indentLevel){
        String indent="\n"+Strings.repeat("\t",indentLevel);

        return new StringBuilder("Sort:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("distinct:").append(distinct)
                .append(indent).append("orderingItem:").append(orderingItem)
                .append(indent).append("keyColumns:").append(Arrays.toString(keyColumns))
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }

    @SuppressWarnings({"rawtypes","unchecked"})
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        OperationContext operationContext=dsp.createOperationContext(this);
        DataSet dataSet=source.getDataSet(dsp);

        if (distinct) {
            dataSet = dataSet.distinct(OperationContext.Scope.DISTINCT.displayName(),
                false, operationContext, true, OperationContext.Scope.DISTINCT.displayName());
            try {
                //operationContext.pushScopeForOp(OperationContext.Scope.LOCATE);
                dataSet = dataSet.map(new SetCurrentLocatedRowFunction(operationContext), true);

            } finally {
               // operationContext.popScope();
            }
        }

        //operationContext.pushScopeForOp(OperationContext.Scope.SORT_KEYER);
        KeyerFunction f=new KeyerFunction(operationContext,keyColumns);
        PairDataSet pair=dataSet.keyBy(f);
        //operationContext.popScope();

        //operationContext.pushScopeForOp(OperationContext.Scope.SHUFFLE);
        PairDataSet sortedByKey=pair.sortByKey(new RowComparator(descColumns,nullsOrderedLow),
            OperationContext.Scope.SORT.displayName());
        //operationContext.popScope();

        //operationContext.pushScopeForOp(OperationContext.Scope.READ_SORTED);
        DataSet sortedValues=sortedByKey.values(OperationContext.Scope.READ_SORTED.displayName());
        //operationContext.popScope();

        try{
            //operationContext.pushScopeForOp(OperationContext.Scope.LOCATE);
            return sortedValues.map(new SetCurrentLocatedRowFunction(operationContext),true);
        }finally{
          //  operationContext.popScope();
        }
    }

    public String getScopeName(){
        return (distinct ? "Sort Distinct" : "Sort");
    }
}