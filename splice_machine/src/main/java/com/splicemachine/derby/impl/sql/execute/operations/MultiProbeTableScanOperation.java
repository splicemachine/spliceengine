
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
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowAndRowKeyFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Result set that fetches rows from a scan by "probing" the underlying
 * table with a given list of values.  Repeated calls to nextRow()
 * will first return all rows matching probeValues[0], then all rows matching
 * probeValues[1], and so on (duplicate probe values are ignored).  Once all
 * matching rows for all values in probeValues have been returned, the call
 * to nextRow() will return null, thereby ending the scan. The
 * expectation is that this kind of result set only ever appears beneath
 * some other top-level result set (esp. IndexRowToBaseRowResultSet), in
 * which case all result sets higher up in the result set tree will just
 * see a stream of rows satisfying the list of probe values.
 *
 * Currently this type of result is used for evaluation of IN lists, where
 * the user wants to retrieve all rows for which some target column has a
 * value that equals one of values in the IN list.  In that case the IN list
 * values are represented by the probeValues array.
 *
 * Most of the work for this class is inherited from TableScanResultSet. 
 * This class overrides four public methods and two protected methods
 * from TableScanResultSet.  In all cases the methods here set probing
 * state and then call the corresponding methods on "super".
 */
public class MultiProbeTableScanOperation extends TableScanOperation  {
    private static final long serialVersionUID = 1l;
    protected int inlistPosition;
//    /**
//     * The values with which we will probe the table, as they were passed to
//     * the constructor. We need to keep them unchanged in case the result set
//     * is reused when a statement is re-executed (see DERBY-827).
//     */
//    protected DataValueDescriptor [] origProbeValues;


//    /**
//     * Tells whether or not we should skip the next attempt to (re)open the
//     * scan controller. If it is {@code true} it means that the previous call
//     * to {@link #initStartAndStopKey()} did not find a new probe value, which
//     * means that the probe list is exhausted and we shouldn't perform a scan.
//     */
//    private boolean skipNextScan;

    /**
     * Only used for Serialization, DO NOT USE
     */
    @Deprecated
    public MultiProbeTableScanOperation() { }

    /**
     * Constructor.  Just save off the relevant probing state and pass
     * everything else up to TableScanResultSet.
     * 
     * @exception StandardException thrown on failure to open
     */
    public MultiProbeTableScanOperation(long conglomId,
        StaticCompiledOpenConglomInfo scoci, Activation activation, 
        GeneratedMethod resultRowAllocator, 
        int resultSetNumber,
        GeneratedMethod startKeyGetter, int startSearchOperator,
        GeneratedMethod stopKeyGetter, int stopSearchOperator,
        boolean sameStartStopPosition,
        boolean rowIdKey,
        String qualifiersField,
        GeneratedMethod getProbingValsFunc,
        int sortRequired,
        int inlistPosition,
        int inlistTypeArrayItem,
        String tableName,
        String userSuppliedOptimizerOverrides,
        String indexName,
        boolean isConstraint,
        boolean forUpdate,
        int colRefItem,
        int indexColItem,
        int lockMode,
        boolean tableLocked,
        int isolationLevel,
        boolean oneRowScan,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost, String tableVersion,
                                        boolean pin,
                                        int splits,
                                        String delimited,
                                        String escaped,
                                        String lines,
                                        String storedAs,
                                        String location,
                                        int partitionByRefItem,
                                        GeneratedMethod defaultRowFunc,
                                        int defaultValueMapItem,
                                        GeneratedMethod pastTxFunctor
                                        )
            throws StandardException
    {
        /* Note: We use '1' as rows per read because we do not currently
         * allow bulk fetching when multi-probing.  If that changes in
         * the future then we will need to update rowsPerRead accordingly.
         */
        super(conglomId,
            scoci,
            activation,
            resultRowAllocator,
            resultSetNumber,
            startKeyGetter,
            startSearchOperator,
            stopKeyGetter,
            stopSearchOperator,
            sameStartStopPosition,
            rowIdKey,
            qualifiersField,
            tableName,
            userSuppliedOptimizerOverrides,
            indexName,
            isConstraint,
            forUpdate,
            colRefItem,
            indexColItem,
            lockMode,
            tableLocked,
            isolationLevel,
            1, // rowsPerRead
            oneRowScan,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost,tableVersion,pin,splits,
                delimited,
                escaped,
                lines,
                storedAs,
                location,
                partitionByRefItem,
                defaultRowFunc,
                defaultValueMapItem,
                pastTxFunctor);

        this.inlistPosition = inlistPosition;

        this.scanInformation = new MultiProbeDerbyScanInformation(
                resultRowAllocator.getMethodName(),
                startKeyGetter==null?null:startKeyGetter.getMethodName(),
                stopKeyGetter==null?null:stopKeyGetter.getMethodName(),
                qualifiersField==null?null:qualifiersField,
                conglomId,
                colRefItem,
                sameStartStopPosition,
                startSearchOperator,
                stopSearchOperator,
                getProbingValsFunc==null?null:getProbingValsFunc.getMethodName(),
                sortRequired,
                inlistPosition,
                inlistTypeArrayItem,
                tableVersion,
                defaultRowFunc==null?null:defaultRowFunc.getMethodName(),
                defaultValueMapItem
        );
        init();
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        inlistPosition = in.readInt();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(inlistPosition);
    }

    @Override
    public String toString() {
        return "MultiProbe"+super.toString();
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        try {
            TxnView txn = getTransaction();
            DataValueDescriptor[] probeValues = ((MultiProbeDerbyScanInformation)scanInformation).getProbeValues();
            List<DataScan> scans = scanInformation.getScans(getCurrentTransaction(), null, activation, getKeyDecodingMap());
            DataSet<ExecRow> dataSet = dsp.getEmpty();
            OperationContext<MultiProbeTableScanOperation> operationContext = dsp.<MultiProbeTableScanOperation>createOperationContext(this);
            dsp.prependSpliceExplainString(this.explainPlan);
            int i = 0;
            List<ScanSetBuilder<ExecRow>> datasets = new ArrayList<>(scans.size());
            for (DataScan scan : scans) {
                deSiify(scan);
                ScanSetBuilder<ExecRow> ssb = dsp.<MultiProbeTableScanOperation, ExecRow>newScanSet(this, tableName)
                        .tableDisplayName(tableDisplayName)
                        .activation(this.getActivation())
                        .transaction(txn)
                        .scan(scan)
                        .template(this.currentTemplate.getClone())
                        .tableVersion(tableVersion)
                        .indexName(indexName)
                        .reuseRowLocation(true)
                        .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                        .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                        .keyColumnTypes(getKeyFormatIds())
                        .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                        .keyDecodingMap(getKeyDecodingMap())
                        .rowDecodingMap(getRowDecodingMap())
                        .baseColumnMap(baseColumnMap)
                        .optionalProbeValue(probeValues[i])
                        .defaultRow(defaultRow, scanInformation.getDefaultValueMap());

                datasets.add(ssb);
                i++;
            }
            // it is possible that all inlist elements are pruned
            if (datasets.isEmpty())
                return dataSet;
            else
                return dataSet.parallelProbe(datasets, operationContext).map(new SetCurrentLocatedRowAndRowKeyFunction<>(operationContext));
        }
        catch (Exception e) {
                throw StandardException.plainWrapException(e);
            }
    }

}
