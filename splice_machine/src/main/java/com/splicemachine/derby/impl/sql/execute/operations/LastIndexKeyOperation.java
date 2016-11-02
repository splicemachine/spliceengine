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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.TakeFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LastIndexKeyOperation extends ScanOperation {
        private static Logger LOG = Logger.getLogger(LastIndexKeyOperation.class);
	    private int[] baseColumnMap;
	    protected static final String NAME = LastIndexKeyOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		public LastIndexKeyOperation() {
        super();
    }

    public LastIndexKeyOperation (
        Activation activation,
        int resultSetNumber,
        GeneratedMethod resultRowAllocator,
        long conglomId,
        String tableName,
        String userSuppliedOptimizerOverrides,
        String indexName,
        int colRefItem,
        int lockMode,
        boolean tableLocked,
        int isolationLevel,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost,
        String tableVersion) throws StandardException {

        super(conglomId, activation, resultSetNumber, null, -1, null, -1,
            true, false, null, resultRowAllocator, lockMode, tableLocked, isolationLevel,

        colRefItem, -1, false,optimizerEstimatedRowCount, optimizerEstimatedCost,tableVersion,false,
                null,null,null,null,null);
        this.tableName = Long.toString(scanInformation.getConglomerateId());
        this.tableDisplayName = tableName;
        this.indexName = indexName;
        init();
    }


    private static final byte [] LAST_ROW = new byte [128];
    static {
        Arrays.fill(LAST_ROW, (byte) 0xff);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
	    this.baseColumnMap = operationInformation.getBaseColumnMap();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableName = in.readUTF();
        if (in.readBoolean())
            indexName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeBoolean(indexName != null);
        if (indexName != null)
            out.writeUTF(indexName);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        return currentTemplate;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet<LocatedRow> scan = dsp.<LastIndexKeyOperation,LocatedRow>newScanSet(this,tableName)
                .tableDisplayName(tableDisplayName)
                .transaction(getCurrentTransaction())
                .scan(getReversedNonSIScan())
                .template(currentTemplate)
                .tableVersion(tableVersion)
                .indexName(indexName)
                .reuseRowLocation(false)
                .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                .keyColumnTypes(getKeyFormatIds())
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(baseColumnMap)
                .buildDataSet(this);

        OperationContext<SpliceOperation> operationContext = dsp.<SpliceOperation>createOperationContext(this);
        return scan.take(new TakeFunction<SpliceOperation, LocatedRow>(operationContext,1))
                .coalesce(1,false)
                .take(new TakeFunction<SpliceOperation, LocatedRow>(operationContext,1));
    }
}


