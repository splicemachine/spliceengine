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

import com.splicemachine.db.iapi.error.ExceptionSeverity;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.splicemachine.db.iapi.error.StandardException.newException;

/**
 * Created by msirek on Nov. 18, 2019.
 */
public class SignalOperation extends NoRowsOperation {
    private static int CURRENT_CLASS_VERSION = 1;
    private int classVersion = CURRENT_CLASS_VERSION;
    private String sqlState;
    private String errorText;
    private String errorTextGeneratorMethodName;
    public SpliceMethod<DataValueDescriptor>  errorTextMethod;
    protected static final String NAME = SignalOperation.class.getSimpleName().replaceAll("Operation", "");

    public SignalOperation() {

    }

    /**
     * Make the ConstantAction for a SIGNAL statement.
     *
     *  @param sqlState	The error code to return.
     */
    public SignalOperation(Activation activation,
                           String sqlState,
                           GeneratedMethod errorTextGenerator) throws StandardException {
        super(activation);
        this.sqlState = sqlState;
        this.errorText = "";
        this.errorTextGeneratorMethodName =
              (errorTextGenerator == null) ? null :
                 errorTextGenerator.getMethodName();
        init();
    }

    public String toString() {
        return "SIGNAL: " + sqlState;
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
                            + "sqlState:" + sqlState + indent
                            + "errorTextGeneratorMethodName:" + errorTextGeneratorMethodName + indent;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);

        if (errorTextGeneratorMethodName != null)
            errorTextMethod = new SpliceMethod<>(errorTextGeneratorMethodName, activation);
    }

    private void throwSignal() throws StandardException {
        StandardException e = newException(sqlState, null, "");
        e.setTextMessage("Application raised error or warning with diagnostic text: \"" + errorText + "\"");
        e.setIsSignal(true);
        e.setSeverity(ExceptionSeverity.STATEMENT_SEVERITY);
        throw e;
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
            sqlState = readNullableString(in);
            errorText = readNullableString(in);
            errorTextGeneratorMethodName = readNullableString(in);
    }

    /**

      @exception IOException thrown on error
     */
    public void writeExternal( ObjectOutput out ) throws IOException {
            out.writeInt(classVersion);
            super.writeExternal(out);
            writeNullableString(sqlState, out);
            writeNullableString(errorText, out);
            writeNullableString(errorTextGeneratorMethodName, out);
    }

    @Override
    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "DB-9844")
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        if (dsp.isSparkExplain())
            return dsp.getEmpty();

        OperationContext<SignalOperation> operationContext =
            dsp.createOperationContext(this);

        DataValueDescriptor errorTextDVD =
            errorTextMethod != null ? errorTextMethod.invoke() : null;
        if (errorTextDVD != null)
            errorText = errorTextDVD.getString();
        throwSignal();

        // We shouldn't reach here, but just in case...
        operationContext.pushScope();
        try {
            return dsp.getEmpty();
        } finally {
            operationContext.popScope();
        }
    }
}
