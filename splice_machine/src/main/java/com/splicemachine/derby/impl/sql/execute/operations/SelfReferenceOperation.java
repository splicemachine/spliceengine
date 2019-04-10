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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * Created by yxia on 3/21/19.
 */
public class SelfReferenceOperation extends SpliceBaseOperation {
    private static Logger LOG = Logger.getLogger(SelfReferenceOperation.class);
    protected static final String NAME = SelfReferenceOperation.class.getSimpleName().replaceAll("Operation","");
    private SpliceOperation recursiveUnionReference = null;
    protected SpliceMethod<ExecRow> rowMethod;
    private ExecRow rowDefinition;
    private String rowMethodName;

    public SelfReferenceOperation() {
        super();
    }

    public SelfReferenceOperation(Activation activation,
                                  GeneratedMethod rowAllocator,
                                  int resultSetNumber,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost) throws StandardException{
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.rowMethodName=rowAllocator.getMethodName();
        init();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        if(rowMethod==null && rowMethodName!=null) {
            this.rowMethod = new SpliceMethod<>(rowMethodName, activation);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        //return Arrays.asList(recursiveUnionReference);
        return Collections.emptyList();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String toString(){
        return "SelfReferenceOperation";
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return "SelfReferenceOperation:" + indent
                + "resultSetNumber:" + resultSetNumber;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return recursiveUnionReference;
    }

    @Override
    public void open() throws StandardException {
        super.open();
    }

    @Override
    public void close() throws StandardException {
        super.close();
    }

    // - - - - - - - - - - - - - - - - - - -
    // serialization
    // - - - - - - - - - - - - - - - - - - -

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.recursiveUnionReference = (SpliceOperation) in.readObject();
        if(in.readBoolean())
            rowMethodName=in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.recursiveUnionReference);
        out.writeBoolean(rowMethodName!=null);
        if(rowMethodName!=null) {
            out.writeUTF(rowMethodName);
        }
    }

    public void writeExternalWithoutChild(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(null);
        out.writeBoolean(rowMethodName!=null);
        if(rowMethodName!=null) {
            out.writeUTF(rowMethodName);
        }
    }

    public ExecRow getRow() throws StandardException{
        if(rowMethod!=null){
            currentRow=rowMethod.invoke();
        }
        return currentRow.getClone();
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        return ((RecursiveUnionOperation)this.recursiveUnionReference).getSelfReference();
    }

    public void setRecursiveUnionReference(NoPutResultSet recursiveUnionReference) {
        if (this.recursiveUnionReference == null)
            this.recursiveUnionReference = (SpliceOperation)recursiveUnionReference;
    }

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
}
