/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.sparkproject.guava.base.Strings;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.compile.ExportNode;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.function.ExportFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * Export the results of an arbitrary SELECT query to HDFS.
 */
public class ExportOperation extends SpliceBaseOperation {

    private static final long serialVersionUID = 0L;

    private SpliceOperation source;
    private ResultColumnDescriptor[] sourceColumnDescriptors;
    private ExportParams exportParams;

    private ExecRow currentTemplate;

    protected static final String NAME = ExportOperation.class.getSimpleName().replaceAll("Operation","");
    private static final Logger LOG = Logger.getLogger(ExportOperation.class);

	@Override
	public String getName() {
			return NAME;
	}
    
    public ExportOperation() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public ExportOperation(SpliceOperation source,
                           ResultColumnDescriptor[] sourceColumnDescriptors,
                           Activation activation,
                           int rsNumber,
                           String exportPath,
                           boolean compression,
                           int replicationCount,
                           String encoding,
                           String fieldSeparator,
                           String quoteCharacter) throws StandardException {
        super(activation, rsNumber, 0d, 0d);

        if (replicationCount <= 0 && replicationCount != ExportNode.DEFAULT_INT_VALUE) {
            throw StandardException.newException(SQLState.EXPORT_PARAMETER_IS_WRONG);
        }

        this.source = source;
        this.sourceColumnDescriptors = sourceColumnDescriptors;
        this.exportParams = new ExportParams(exportPath, compression, replicationCount, encoding, fieldSeparator, quoteCharacter);
        this.activation = activation;

        try {
            ExportPermissionCheck checker = new ExportPermissionCheck(exportParams);
            checker.verify();
            checker.cleanup();
            init();
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        currentTemplate = new ValueRow(0);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.singletonList(source);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentTemplate;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return indent + "resultSetNumber:" + resultSetNumber + indent
                + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    // - - - - - - - - - - - -
    // export only methods
    // - - - - - - - - - - - -

    public ExportParams getExportParams() {
        return exportParams;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public ResultColumnDescriptor[] getSourceResultColumnDescriptors() {
        return this.sourceColumnDescriptors;
    }

    // - - - - - - - - - - - -
    // serialization
    // - - - - - - - - - - - -

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        currentTemplate = (ExecRow) in.readObject();
        exportParams = (ExportParams) in.readObject();
        int srcColDescriptors = in.readInt();
        sourceColumnDescriptors = new ResultColumnDescriptor[srcColDescriptors];

        for (int i = 0; i < srcColDescriptors; i++) {
            sourceColumnDescriptors[i] = (ResultColumnDescriptor) in.readObject();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeObject(currentTemplate);
        out.writeObject(exportParams);
        out.writeInt(sourceColumnDescriptors.length);
        for (int i = 0; i < sourceColumnDescriptors.length; i++) {
            out.writeObject(sourceColumnDescriptors[i]);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getDataSet(): begin");
        DataSet<LocatedRow> dataset = source.getDataSet(dsp);
        OperationContext<ExportOperation> operationContext = dsp.createOperationContext(this);
        DataSetWriter writer = dataset.writeToDisk()
            .directory(exportParams.getDirectory())
            .exportFunction(new ExportFunction(operationContext))
            .build();
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getDataSet(): writing");
        operationContext.pushScope();
        try {
            DataSet<LocatedRow> resultDs = writer.write();
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "getDataSet(): done");
            return resultDs;
        } finally {
            operationContext.popScope();
        }
    }
}
