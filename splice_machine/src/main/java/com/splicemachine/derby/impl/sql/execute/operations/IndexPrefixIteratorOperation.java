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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowAndRowKeyFunction;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowFunction;
import com.splicemachine.derby.stream.function.driver.IndexPrefixIteratorFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.splicemachine.EngineDriver.isMemPlatform;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

/**
 *
 * Operation for collecting an index's first column values
 * to use as index start key prefixes for driving a TableScan or IndexScan.
 *
 */
public class IndexPrefixIteratorOperation extends TableScanOperation{
    private static final long serialVersionUID=3l;
    private static Logger LOG=Logger.getLogger(IndexPrefixIteratorOperation.class);
    private SpliceOperation sourceResultSet = null;
    protected static final String opName=IndexPrefixIteratorOperation.class.getSimpleName().replaceAll("Operation","");
    private int firstIndexColumnNumber;
    private ScanSetBuilder scanSetBuilder;
    private DataSetProcessor controlDSP;
    private List<ExecRow> keys;  // The values from the first column in the index to use in start keys.
    boolean isMemPlatform = isMemPlatform();

    @Override
    public String getName(){
        return opName;
    }

    /**
     * Empty Constructor
     *
     */
    public IndexPrefixIteratorOperation(){
        super();
    }

    public IndexPrefixIteratorOperation(
                              SpliceOperation sourceResultSet,
                              int firstIndexColumnNumber,
                              long conglomId,
                              StaticCompiledOpenConglomInfo scoci,
                              Activation activation,
                              GeneratedMethod resultRowAllocator,
                              int resultSetNumber,
                              GeneratedMethod startKeyGetter,
                              int startSearchOperator,
                              GeneratedMethod stopKeyGetter,
                              int stopSearchOperator,
                              boolean sameStartStopPosition,
                              boolean rowIdKey,
                              String qualifiersField,
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
                              int rowsPerRead,
                              boolean oneRowScan,
                              double optimizerEstimatedRowCount,
                              double optimizerEstimatedCost,
                              String tableVersion,
                              int splits,
                              String delimited,
                              String escaped,
                              String lines,
                              String storedAs,
                              String location,
                              int partitionByRefItem,
                              GeneratedMethod defaultRowFunc,
                              int defaultValueMapItem,
                              GeneratedMethod pastTxFunctor,
                              long minRetentionPeriod,
                              int numUnusedLeadingIndexFields) throws StandardException{
                super(conglomId, scoci, activation, resultRowAllocator, resultSetNumber, startKeyGetter,
                      startSearchOperator, stopKeyGetter, stopSearchOperator, sameStartStopPosition,
                      rowIdKey, qualifiersField, tableName, userSuppliedOptimizerOverrides, indexName,
                      isConstraint, forUpdate, colRefItem, indexColItem, lockMode, tableLocked,
                      isolationLevel, rowsPerRead, oneRowScan, optimizerEstimatedRowCount,
                      optimizerEstimatedCost, tableVersion, splits, delimited, escaped,
                      lines, storedAs, location, partitionByRefItem, defaultRowFunc,
                      defaultValueMapItem, pastTxFunctor, minRetentionPeriod, numUnusedLeadingIndexFields);
        SpliceLogUtils.trace(LOG,"instantiated for tablename %s or indexName %s with conglomerateID %d",
                tableName,indexName,conglomId);
        this.sourceResultSet = sourceResultSet;
        this.firstIndexColumnNumber = firstIndexColumnNumber;
    }

    /**
     *
     * Serialization/Deserialization
     *
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        firstIndexColumnNumber = in.readInt();
    }
    /**
     *
     * Serialization/Deserialization
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(firstIndexColumnNumber);
    }

    /**
     *
     * Initialize variables after creation or serialization.
     *
     * @param context
     * @throws StandardException
     * @throws IOException
     */
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
    }

    /**
     *
     * Prints the name for explain plan.
     *
     * @param indentLevel
     * @return
     */
    @Override
    public String prettyPrint(int indentLevel){
        return "IndexPrefixIteratorOperation";
    }


    private void closeFirstTableScanner(TableScannerIterator tableScannerIterator) throws StandardException {
        try {
            tableScannerIterator.close();
        }
        catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }

    }

    /**
     *
     * Retrieve the DataSet abstraction for this table scan.
     *
     * @param dsp
     * @return
     * @throws StandardException
     */
    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        assert currentTemplate!=null:"Current Template Cannot Be Null";

        oneRowScan = true;
        operationContext = dsp.createOperationContext(this);

        if (keys == null) {
            DataSet<ExecRow> ds = getDriverDataSet(createTableScannerBuilder());
            TableScannerIterator tableScannerIterator = (TableScannerIterator) ds.toLocalIterator();
            registerCloseable(tableScannerIterator);
            // Most of the probing logic is in IndexPrefixIteratorFunction, as created below.
            if (isMemPlatform)
                ds = ds.mapPartitions(new IndexPrefixIteratorFunction(operationContext, firstIndexColumnNumber), true);

            // Only grab the first row for non-mem platforms.
            // Use the new custom HBase filter in this case, which needs
            // the DataValueDescriptor of the first index column to
            // be passed to the constructor.
            if (!isMemPlatform) {
                ExecRow firstRow = null;
                if (tableScannerIterator.hasNext())
                    firstRow = tableScannerIterator.next();
                closeFirstTableScanner(tableScannerIterator);
                if (firstRow == null)
                    return controlDSP.getEmpty();

                int firstMappedIndexColumnNumber = baseColumnMap[firstIndexColumnNumber-1]+1;
                ExecRow keyRow = new ValueRow(1);
                keyRow.setColumn(1, firstRow.getColumn(firstMappedIndexColumnNumber));
                keys = new ArrayList<>();
                keys.add(keyRow);
            }
            else
                keys = ds.collect();

            closeFirstTableScanner(tableScannerIterator);

            // IndexPrefixIteratorFunction has set scanKeyPrefix.
            // Future operations won't want this set, so reset it back to null.
            ((BaseActivation) getActivation()).setScanKeyPrefix(null);
        }
        if (!isMemPlatform && keys.size() > 0) {
            ExecRow firstRow = keys.get(0);

            if (!(sourceResultSet instanceof TableScanOperation))
                throw StandardException.newException(LANG_INTERNAL_ERROR,
                    "Attempt to apply index prefix iteration on something other than a table scan.");

            TableScanOperation tableScan = (TableScanOperation) sourceResultSet;
            tableScan.setFirstRowOfIndexPrefixIteration(firstRow);

            // Give the source result set access to the prefix keys.
            ((BaseActivation) sourceResultSet.getActivation()).setFirstIndexColumnKeys(keys);
            ((BaseActivation) sourceResultSet.getActivation()).setSkipBuildOfFirstKeyColumn(true);
            DataSet<ExecRow> dataSet = tableScan.getDataSet(dsp);
            ((BaseActivation) sourceResultSet.getActivation()).setFirstIndexColumnKeys(null);
            ((BaseActivation) sourceResultSet.getActivation()).setSkipBuildOfFirstKeyColumn(false);
            tableScan.setFirstRowOfIndexPrefixIteration(null);
            return dataSet;
        }

        DataSet<ExecRow> finalDS;
        if (keys.size() == 0)
            finalDS = controlDSP.getEmpty();
        else if (isMemPlatform) {
            ((BaseActivation) sourceResultSet.getActivation()).setSameStartStopScanKeyPrefix(true);
            // Instead of using MultiRowRangeFilter, on mem we create a separate scanner for each
            // key and call setScanKeyPrefix in order to combine it with the remainder of the start key.
            // Setting setSameStartStopScanKeyPrefix causes the scanKeyPrefix to include the start key.
            // Otherwise, scanKeyPrefix is not combined with a key suffix, and the scan searches
            // for rows with rowkey greater than scanKeyPrefix.  The main use of that mode is in
            // IndexPrefixIteratorFunction, where we jump from one key to the next, collecting
            // up all of the keys which have rows on disk.
            IteratorChain unionedDataSets = new IteratorChain();
            for (ExecRow keyRow:keys) {
                ((BaseActivation) sourceResultSet.getActivation()).setScanKeyPrefix(keyRow.getColumn(1));
                DataSet<ExecRow> tempDS = sourceResultSet.getDataSet(dsp);
                Iterator tableIterator = tempDS.toLocalIterator();
                unionedDataSets.addIterator(tableIterator);
            }
            ((BaseActivation) sourceResultSet.getActivation()).setSameStartStopScanKeyPrefix(false);
            ((BaseActivation) sourceResultSet.getActivation()).setScanKeyPrefix(null);
            finalDS = controlDSP.createDataSet(unionedDataSets);
        }
        else {
            // Give the source result set access to the prefix keys.
            ((BaseActivation) sourceResultSet.getActivation()).setFirstIndexColumnKeys(keys);
            finalDS = sourceResultSet.getDataSet(dsp);
            ((BaseActivation)sourceResultSet.getActivation()).setFirstIndexColumnKeys(null);
        }
        return finalDS;
    }

    /**
     * @return the string representation for TableScan.
     */
    @Override
    public String toString(){
        try{
            return String.format("IndexPrefixIteratorOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,scanInformation.isKeyed(),resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }catch(Exception e){
            return String.format("IndexPrefixIteratorOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,"UNKNOWN",resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }
    }

    /**
     * @return the Table Scan Builder for returning the first row in a data set.
     */
    public ScanSetBuilder<ExecRow> createTableScannerBuilder() throws StandardException{
        TxnView txn = getCurrentTransaction();

        if (controlDSP == null)
            controlDSP =
            EngineDriver.driver().processorFactory().
                                                    localProcessor(getOperation().getActivation(), this);
        DataScan dataScan = getNonSIScan();

        // No need for a large cache since we're
        // going after a single row on each read.
        dataScan.cacheRows(2).batchCells(-1);

        scanSetBuilder =
        controlDSP.<TableScanOperation,ExecRow>newScanSet(this,tableName)
                .tableDisplayName(tableDisplayName)
                .activation(activation)
                .transaction(txn)
                .scan(dataScan)
                .template(currentTemplate)
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
                .delimited(delimited)
                .escaped(escaped)
                .lines(lines)
                .storedAs(storedAs)
                .location(location)
                .partitionByColumns(getPartitionColumnMap())
                .defaultRow(defaultRow,scanInformation.getDefaultValueMap())
                .ignoreRecentTransactions(isReadOnly(txn));

        return scanSetBuilder;
    }

    @Override
    public DataSet<ExecRow> getTableScannerBuilder(DataSetProcessor dsp) throws StandardException{
        ScanSetBuilder<ExecRow> ssb = createTableScannerBuilder();
        return getDriverDataSet(ssb);
    }

    public DataSet<ExecRow> getDriverDataSet(ScanSetBuilder<ExecRow> scanSetBuilder)
                                             throws StandardException {

        DataSet<ExecRow> dataSet = scanSetBuilder.buildDataSet(this);
        return dataSet;
    }

    public int getFirstIndexColumnNumber() {
        return firstIndexColumnNumber;
    }

    public ScanSetBuilder getScanSetBuilder() {
        return scanSetBuilder;
    }
}
