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

package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.LeadLagFunctionDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LeadLagFunction extends SpliceGenericWindowFunction {
    private int offset;
    // TODO: JC - handle default value
    private DataValueDescriptor defaultValue;
    private LeadLagBuffer buffer;

    @Override
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(cf, aggregateName, returnType);
        boolean isLead = aggregateName.equals("LEAD");
        this.offset = (int) functionSpecificArgs.get(LeadLagFunctionDefinition.OFFSET);
        // TODO: JC - handle default value
//        this.defaultValue = (DataValueDescriptor) functionSpecificArgs.get(LeadLagFunctionDefinition.DEFAULT_VALUE);
        try {
            this.defaultValue = returnType.getNull();
        } catch (StandardException e) {
            // TODO: Add StandardException to WindowFunction.setup()?
            throw new RuntimeException("Error initializing LeadLag function :"+ e.getLocalizedMessage());
        }
        if (isLead) {
            buffer = new LeadBuffer();
        } else {
            buffer = new LagBuffer();
        }
        buffer.initialize(offset);
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
        buffer.addRow(valueDescriptors[0], defaultValue);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        // just set default results here, otherwise framework will break
        chunk.setResult(dvds[0]);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        // nothing to do here
    }

    @Override
    public List<DataValueDescriptor> finishFrame() throws StandardException {
        List<DataValueDescriptor> leadLag = buffer.terminate();
        buffer.initialize(offset);
        for(WindowChunk chunk : chunks) {
            for (int i = chunk.first; i < chunk.last; i++) {
                chunk.setResult(chunk.values[i][0]);
            }
        }
        return leadLag;
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        if (chunks.isEmpty() || chunks.get(0).isEmpty())
            return null;
        return chunks.get(0).getResult();
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new LeadLagFunction();
    }

    /**
     * Add, evaluate and determine lead/lag values and buffer the the results for a given
     * window frame.
     */
    interface LeadLagBuffer {
        /**
         * Call this after {@link #terminate()} to remove prior collected values.
         * @param leadLagAmt the lead/lage offset
         */
        void initialize(int leadLagAmt);

        /**
         * Add the current value for this row and the default value. The default value
         * will be set to the lead/lag row's current value if/when we get there.
         * @param leadExprValue the current row's value
         * @param defaultValue the default lead/lag value in case we don't get to the
         *                     offset row
         */
        void addRow(DataValueDescriptor leadExprValue, DataValueDescriptor defaultValue);

        /**
         * Call this when you're finished with the frame to get the lead/lag values.
         * <b>NOTE</b>: This does <b>not</b> clear any values or reset in any way. You'll
         * need to do that when moving to a new frame before adding new rows. This is for
         * performance reasons - we don't attempt to anticipate what you want to do.
         * @return the lead/lag result values evaluated in a frame.
         */
        List<DataValueDescriptor> terminate();

    }


    static class LeadBuffer implements LeadLagBuffer {
        ArrayList<DataValueDescriptor> values;
        int leadAmt;
        DataValueDescriptor[] leadWindow;
        int nextPosInWindow;
        int lastRowIdx;

        public void initialize(int leadAmt) {
            if (leadAmt < 0) {
                throw new RuntimeException("LEAD() offset must be positive.");
            }
            this.leadAmt = leadAmt;
            values = new ArrayList<>();
            leadWindow = new DataValueDescriptor[(leadAmt > 0 ? leadAmt : 1)];
            nextPosInWindow = 0;
            lastRowIdx = -1;
        }

        public void addRow(DataValueDescriptor leadExprValue, DataValueDescriptor defaultValue) {
            int row = lastRowIdx + 1;
            int leadRow = row - leadAmt;
            if ( leadRow >= 0 ) {
                values.add(leadExprValue);
            }
            leadWindow[nextPosInWindow] = defaultValue;
            nextPosInWindow = (nextPosInWindow + 1) % (leadAmt > 0 ? leadAmt : 1);
            lastRowIdx++;
        }

        public List<DataValueDescriptor> terminate() {
            if (values == null) {
                return new ArrayList<>(0);
            }
          /*
           * if there are fewer than leadAmt values in leadWindow; start reading from the first position.
           * Otherwise the window starts from nextPosInWindow.
           */
            if ( lastRowIdx < leadAmt ) {
                nextPosInWindow = 0;
            }
            for(int i=0; i < leadAmt; i++) {
                DataValueDescriptor value = leadWindow[nextPosInWindow];
                if (value != null) {
                    values.add(value);
                }
                nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
            }
            return values;
        }

    }

    static class LagBuffer implements LeadLagBuffer {
        ArrayList<DataValueDescriptor> values;
        int lagAmt;
        ArrayList<DataValueDescriptor> lagValues;
        int lastRowIdx;

        public void initialize(int lagAmt) {
            if (lagAmt < 0) {
                throw new RuntimeException("LAG() offset must be positive.");
            }
            this.lagAmt = lagAmt;
            lagValues = new ArrayList<>(lagAmt);
            values = new ArrayList<>();
            lastRowIdx = -1;
        }

        public void addRow(DataValueDescriptor currValue, DataValueDescriptor defaultValue) {
            int row = lastRowIdx + 1;
            if ( row < lagAmt) {
                lagValues.add(defaultValue);
            }
            values.add(currValue);
            lastRowIdx++;
        }

        public List<DataValueDescriptor> terminate() {
          /*
           * if partition is smaller than the lagAmt;
           * the entire partition is in lagValues.
           */
            if (values != null) {
                if ( values.size() < lagAmt ) {
                    values = lagValues;
                    return lagValues;
                }
                int lastIdx = values.size() - 1;
                for(int i = 0; i < lagAmt; i++) {
                    values.remove(lastIdx - i);
                }
                values.addAll(0, lagValues);
                return values;
            }
            return new ArrayList<>(0);
        }
    }

}
