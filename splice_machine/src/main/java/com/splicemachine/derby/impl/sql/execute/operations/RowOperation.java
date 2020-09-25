/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import splice.com.google.common.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.function.RowOperationFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
    /**
    * Takes a constant row value and returns it as
    * a result set.
    * <p>
    * This class actually probably never underlies a select statement,
    * but in case it might and because it has the same behavior as the
    * ones that do, we have it implement CursorResultSet and give
    * reasonable answers.
    *
    */
public class RowOperation extends SpliceBaseOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(RowOperation.class);
    protected boolean canCacheRow;
    protected boolean next=false;
    protected SpliceMethod<ExecRow> rowMethod;
    protected ExecRow cachedRow;
    private ExecRow rowDefinition;
    private String rowMethodName; //name of the row method for
    protected static final String NAME=RowOperation.class.getSimpleName().replaceAll("Operation","");
    private boolean skipClone = false;

    /**
     *
     * Retrieve the static name for the operation.
     *
     * @return
     */
    @Override
    public String getName(){
        return NAME;
    }


    /**
     * Required for serialization...
     */
    public RowOperation(){

    }

    /**
     *
     * Instance method for projected value.
     *
     * @param activation
     * @param row
     * @param canCacheRow
     * @param resultSetNumber
     * @param optimizerEstimatedRowCount
     * @param optimizerEstimatedCost
     * @throws StandardException
     */
    public RowOperation(
            Activation activation,
            GeneratedMethod row,
            boolean canCacheRow,
            int resultSetNumber,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost) throws StandardException{
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.canCacheRow=canCacheRow;
        this.rowMethodName=row.getMethodName();
        init();
    }

    /**
     *
     * Instance method for straight forward non-projected value.
     *
     * @param activation
     * @param constantRow
     * @param canCacheRow
     * @param resultSetNumber
     * @param optimizerEstimatedRowCount
     * @param optimizerEstimatedCost
     * @throws StandardException
     */
    public RowOperation(
            Activation activation,
            ExecRow constantRow,
            boolean canCacheRow,
            int resultSetNumber,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost) throws StandardException{
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.cachedRow=constantRow;
        this.canCacheRow=canCacheRow;
        init();
    }

    /**
     *
     * Called during creation and after serialization.
     *
     * @param context
     * @throws StandardException
     * @throws IOException
     */
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        if(rowMethod==null && rowMethodName!=null){
            this.rowMethod=new SpliceMethod<>(rowMethodName,activation);
        }

        if (activation != null) {
            DMLWriteOperation op;
            if (activation.getResultSet() instanceof DMLWriteOperation) {
                op = (DMLWriteOperation) activation.getResultSet();
                if (op.hasGenerationClause() && op.hasStatementTriggerWithReferencingClause())
                    skipClone = true;
            }
        }
    }

    /**
     *
     * Serde.
     *
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        SpliceLogUtils.trace(LOG,"readExternal");
        super.readExternal(in);
        canCacheRow=in.readBoolean();
        next=in.readBoolean();
        if(in.readBoolean())
            rowMethodName=in.readUTF();
        skipClone = in.readBoolean();
    }

    /**
     *
     * Serde.
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SpliceLogUtils.trace(LOG,"writeExternal");
        super.writeExternal(out);
        out.writeBoolean(canCacheRow);
        out.writeBoolean(next);
        out.writeBoolean(rowMethodName!=null);
        if(rowMethodName!=null){
            out.writeUTF(rowMethodName);
        }
        out.writeBoolean(skipClone);
    }

    /**
     *
     * Retrieve the row for the operation.
     *
     * @return
     * @throws StandardException
     */
    public ExecRow getRow() throws StandardException{
        if(cachedRow!=null){
            SpliceLogUtils.trace(LOG,"getRow,cachedRow=%s",cachedRow);
            return cachedRow.getClone();
        }

        if(rowMethod!=null){
            currentRow=rowMethod.invoke();
            if(canCacheRow){
                cachedRow=currentRow;
            }
        }
        // Don't return a clone of the current row, if we need to
        // modify it to fill in the generated columns later.
        // Future accesses to the row need the full row with
        // generated values filled in.
        if (skipClone)
            return currentRow;
        else
            return currentRow.getClone();
    }

    /**
     * This is not operating against a stored table,
     * so it has no row location to report.
     *
     * @return a null.
     * @see CursorResultSet
     */
    public RowLocation getRowLocation(){
        return null;
    }


    /**
     *
     * No Sub operations under a simple row.
     *
     * @return
     */
    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.emptyList();
    }

    /**
     *
     * String Representation of a cached row.
     *
      * @return
     */
    @Override
    public String toString(){
        return "RowOp {cachedRow="+cachedRow+"}";
    }

    /**
     *
     * ExecRow Definition of the cached row (types, etc.)
     *
     * @return
     * @throws StandardException
     */
    @Override
    public ExecRow getExecRowDefinition() throws StandardException{
        if(rowDefinition==null){
            ExecRow templateRow=getRow();
            if(templateRow!=null){
                rowDefinition=templateRow.getClone();
            }
            SpliceLogUtils.trace(LOG,"execRowDefinition=%s",rowDefinition);
        }
        return rowDefinition;
    }

    @Override
    public String prettyPrint(int indentLevel){
        String indent="\n"+Strings.repeat("\t",indentLevel);

        return new StringBuilder("RowOp:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("canCacheRow:").append(canCacheRow)
                .append(indent).append("rowMethodName:").append(rowMethodName)
                .toString();
    }

        /**
         *
         * Get the Root AccessedCols.  Always null for RowOperation.
         *
          * @param tableNumber
         * @return
         */
    @Override
    public int[] getRootAccessedCols(long tableNumber){
        return null;
    }

    /**
     *
     * Does it reference the table.  Fale for RowOperation.
     *
     * @param tableNumber
     * @return
     */
    @Override
    public boolean isReferencingTable(long tableNumber){
        return false;
    }
    /**
     *
     * Return the dataset abstraction for the operation.
     *
     * @param dsp
     * @return
     * @throws StandardException
     */
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        ExecRow execRow=new ValueRow(1);
        execRow.setColumn(1,new SQLInteger(123));
        dsp.prependSpliceExplainString(this.explainPlan);
        return dsp.singleRowDataSet(execRow)
                .map(new RowOperationFunction(dsp.createOperationContext(this)));
    }

}
