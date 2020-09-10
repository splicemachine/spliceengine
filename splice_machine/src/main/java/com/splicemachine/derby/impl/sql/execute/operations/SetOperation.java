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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Created by msirek on Nov. 23, 2019.
 */
public class SetOperation extends NoRowsOperation {

    private static int CURRENT_CLASS_VERSION = 1;
    private int classVersion = CURRENT_CLASS_VERSION;

    private String getColumnDVDsMethodNames;
    private String [] getColumnDVDsMethodName;
    private String getNewDVDsMethodName;

    protected static final String NAME = SetOperation.class.getSimpleName().replaceAll("Operation", "");

    public SetOperation() {

    }

    private void setupGetColumnDVDsMethodNames() {
        this.getColumnDVDsMethodName = getColumnDVDsMethodNames.split("\\.");
    }

    /**
     * Make the Operation for a SET statement.
     *
     *  @param getColumnDVDsMethodNames	The names of the methods used to access the columns in the NEW row, separated by periods.
     */
    public SetOperation(Activation activation,
                        String getColumnDVDsMethodNames,
                        GeneratedMethod getNewDVDsMethod) throws StandardException {
        super(activation);
        this.getColumnDVDsMethodNames = getColumnDVDsMethodNames;
        this.getNewDVDsMethodName = getNewDVDsMethod.getMethodName();
        setupGetColumnDVDsMethodNames();

        init();
    }

    public	String	toString() {
        return "SET: ";
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String prettyPrint(int indentLevel) {
                    String indent = "\n"+ Strings.repeat("\t",indentLevel);

                    return "Signal:" + indent
                            + "getColumnDVDsMethodNames:" + getColumnDVDsMethodNames + indent
                            + "getNewDVDsMethodName:" + getNewDVDsMethodName + indent;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
    }

    /**
      @see java.io.Externalizable#readExternal
      @exception IOException thrown on error
      @exception ClassNotFoundException	thrown on error
     */
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
            // Read in the serialized class version, but don't use it (for now).
            in.readInt();
            super.readExternal(in);
            getColumnDVDsMethodNames = readNullableString(in);
            getNewDVDsMethodName = readNullableString(in);
            setupGetColumnDVDsMethodNames();
    }

    /**

      @exception IOException thrown on error
     */
    public void writeExternal( ObjectOutput out ) throws IOException {
            out.writeInt(classVersion);
            super.writeExternal(out);
            writeNullableString(getColumnDVDsMethodNames, out);
            writeNullableString(getNewDVDsMethodName, out);

    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        if (dsp.isSparkExplain())
            return dsp.getEmpty();

        SpliceMethod<DataValueDescriptor[]> getNewDVDsMethod = new SpliceMethod<>(getNewDVDsMethodName, activation);
        DataValueDescriptor [] columnDVDs = new DataValueDescriptor[getColumnDVDsMethodName.length];
        DataValueDescriptor [] replacementValues = getNewDVDsMethod.invoke();
        if (columnDVDs.length != replacementValues.length)
            throw new RuntimeException();

        OperationContext<SetOperation> operationContext =
            dsp.createOperationContext(this);

        for (int i = 0; i < columnDVDs.length; i++) {
            SpliceMethod<DataValueDescriptor> getColumnDVDsMethod =
                new SpliceMethod<DataValueDescriptor>(getColumnDVDsMethodName[i], activation);
            columnDVDs[i] = getColumnDVDsMethod.invoke();
            DataValueDescriptor itemToUpdate = ((DataValueDescriptor)columnDVDs[i].getObject());
            itemToUpdate.setValue(replacementValues[i]);
        }

        operationContext.pushScope();
        try {
            return dsp.getEmpty();
        } finally {
            operationContext.popScope();
        }
    }
}
