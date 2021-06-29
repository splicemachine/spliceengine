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
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.pipeline.Exceptions;

import java.io.IOException;
import java.util.*;

/**
 * Operation for holding in-memory result set
 *
 * @author P Trolard
 *         Date: 04/02/2014
 */
public class CursorOperation extends SpliceBaseOperation {

    protected static final String NAME = CursorOperation.class.getSimpleName().replaceAll("Operation", "");
    CursorResultSet sourceRS;

    @Override
    public String getName() {
        return NAME;
    }


    public CursorOperation() {
    }

    public CursorOperation(Activation activation, CursorResultSet sourceRS) throws StandardException {
        super(activation, 0, 0, 0);
        this.sourceRS = sourceRS;
        sourceRS.open();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        try {
            super.init(context);
        }
        catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }


    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return sourceRS.getCurrentRow();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "";
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        Iterator<ExecRow> i =  new Iterator<ExecRow>() {
                @Override
                public boolean hasNext() {
                    try {
                        return sourceRS.getNextRow() != null;
                    } catch (StandardException e) {
                        return false;
                    }
                }

                @Override
                public ExecRow next() {
                    try {
                        return sourceRS.getCurrentRow();
                    } catch (StandardException e) {
                        return null;
                    }
                }
            };
        return dsp.createDataSet(i);
    }

    @Override
    public void close() throws StandardException {
        sourceRS.close();
    }
}
