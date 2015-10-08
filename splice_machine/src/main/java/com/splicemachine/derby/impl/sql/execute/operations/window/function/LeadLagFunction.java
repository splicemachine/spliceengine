package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.LeadLagFunctionDefinition;

/**
 * @author Jeff Cunningham
 *         Date: 9/30/15
 */
public class LeadLagFunction extends SpliceGenericWindowFunction {
    private boolean isLead;
    private int offset;
    // TODO: JC - handle default value
    private DataValueDescriptor defaultValue;
    private LeadLagBuffer buffer;

    @Override
    public WindowFunction setup(ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(cf, aggregateName, returnType);
        this.isLead = aggregateName.equals("LEAD");
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
    public void finishFrame() throws StandardException {
        List<DataValueDescriptor> leadLag = buffer.terminate();
        buffer.initialize(offset);
        for(WindowChunk chunk : chunks) {
            for (int i = chunk.first; i < chunk.last; i++) {
                chunk.setResult(chunk.values[i][0]);
            }
        }
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        return chunks.get(0).getResult();
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new LeadLagFunction();
    }

    interface LeadLagBuffer {
        void initialize(int leadAmt);

        void addRow(DataValueDescriptor leadExprValue, DataValueDescriptor defaultValue);

        List<DataValueDescriptor> terminate();

    }


    static class LeadBuffer implements LeadLagBuffer {
        ArrayList<DataValueDescriptor> values;
        int leadAmt;
        DataValueDescriptor[] leadWindow;
        int nextPosInWindow;
        int lastRowIdx;

        public void initialize(int leadAmt) {
            this.leadAmt = leadAmt;
            values = new ArrayList<>();
            leadWindow = new DataValueDescriptor[leadAmt];
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
            nextPosInWindow = (nextPosInWindow + 1) % leadAmt;
            lastRowIdx++;
        }

        public List<DataValueDescriptor> terminate() {
          /*
           * if there are fewer than leadAmt values in leadWindow; start reading from the first position.
           * Otherwise the window starts from nextPosInWindow.
           */
            if ( lastRowIdx < leadAmt ) {
                nextPosInWindow = 0;
            }
            for(int i=0; i < leadAmt; i++) {
                values.add(leadWindow[nextPosInWindow]);
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
    }

}
