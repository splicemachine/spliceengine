/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Strings;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.GenericPreparedStatement;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.Restriction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.function.IndexToBaseRowFilterPredicateFunction;
import com.splicemachine.derby.stream.function.IndexToBaseRowFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Maps between an Index Table and a data Table.
 */
public class IndexRowToBaseRowOperation extends SpliceBaseOperation{
    private static Logger LOG = Logger.getLogger(IndexRowToBaseRowOperation.class);
    protected int lockMode;
    protected int isolationLevel;
    protected FormatableBitSet accessedCols;
    protected String resultRowAllocatorMethodName;
    protected StaticCompiledOpenConglomInfo scoci;
    protected DynamicCompiledOpenConglomInfo dcoci;
    protected SpliceOperation source;
    protected String indexName;
    protected boolean forUpdate;
    protected SpliceMethod<DataValueDescriptor> restriction;
    protected String restrictionMethodName;
    protected FormatableBitSet accessedHeapCols;
    protected FormatableBitSet heapOnlyCols;
    protected FormatableBitSet accessedAllCols;
    protected int[] indexCols;
    protected ExecRow resultRow;
    protected DataValueDescriptor[]	rowArray;
    protected int scociItem;
    protected long conglomId;
    protected int heapColRefItem;
    protected int allColRefItem;
    protected int heapOnlyColRefItem;
    protected int indexColMapItem;
    private ExecRow compactRow;
    int[] columnOrdering;
    int[] format_ids;
    SpliceConglomerate conglomerate;
    /*
        * Variable here to stash pre-generated DataValue definitions for use in
        * getExecRowDefinition(). Save a little bit of performance by caching it
        * once created.
        */
    private int[] adjustedBaseColumnMap;

//    private static final MetricName scanName = new MetricName("com.splicemachine.operations","indexLookup","totalTime");
//    private final Timer totalTimer = SpliceDriver.driver().getRegistry().newTimer(scanName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    private IndexRowReaderBuilder readerBuilder;
    private String tableVersion;

    protected static final String NAME = "IndexLookup";

    @Override
    public String getName() {
        return NAME;
    }


    public IndexRowToBaseRowOperation () {
        super();
    }

    public IndexRowToBaseRowOperation(long conglomId, int scociItem,
                                      Activation activation, SpliceOperation source,
                                      GeneratedMethod resultRowAllocator, int resultSetNumber,
                                      String indexName, int heapColRefItem, int allColRefItem,
                                      int heapOnlyColRefItem, int indexColMapItem,
                                      GeneratedMethod restriction, boolean forUpdate,
                                      double optimizerEstimatedRowCount, double optimizerEstimatedCost, String tableVersion) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        SpliceLogUtils.trace(LOG,"instantiate with parameters");
        this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
        this.source = source;
        this.indexName = indexName;
        this.forUpdate = forUpdate;
        this.scociItem = scociItem;
        this.conglomId = conglomId;
        this.heapColRefItem = heapColRefItem;
        this.allColRefItem = allColRefItem;
        this.heapOnlyColRefItem = heapOnlyColRefItem;
        this.indexColMapItem = indexColMapItem;
        this.restrictionMethodName = restriction==null? null: restriction.getMethodName();
        this.tableVersion = tableVersion;
        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scociItem = in.readInt();
        conglomId = in.readLong();
        heapColRefItem = in.readInt();
        allColRefItem = in.readInt();
        heapOnlyColRefItem = in.readInt();
        indexColMapItem = in.readInt();
        source = (SpliceOperation) in.readObject();
        accessedCols = (FormatableBitSet) in.readObject();
        resultRowAllocatorMethodName = in.readUTF();
        indexName = in.readUTF();
        restrictionMethodName = readNullableString(in);
        tableVersion = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(scociItem);
        out.writeLong(conglomId);
        out.writeInt(heapColRefItem);
        out.writeInt(allColRefItem);
        out.writeInt(heapOnlyColRefItem);
        out.writeInt(indexColMapItem);
        out.writeObject(source);
        out.writeObject(accessedCols);
        out.writeUTF(resultRowAllocatorMethodName);
        out.writeUTF(indexName);
        writeNullableString(restrictionMethodName, out);
        out.writeUTF(tableVersion);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        try {
            if(restrictionMethodName !=null){
                SpliceLogUtils.trace(LOG,"%s:restrictionMethodName=%s",indexName,restrictionMethodName);
                restriction = new SpliceMethod<DataValueDescriptor>(restrictionMethodName,activation);
            }
            SpliceMethod<ExecRow> generatedMethod = new SpliceMethod<ExecRow>(resultRowAllocatorMethodName,activation);
            final GenericPreparedStatement gp = (GenericPreparedStatement)activation.getPreparedStatement();
            final Object[] saved = gp.getSavedObjects();
            scoci = (StaticCompiledOpenConglomInfo)saved[scociItem];
            TransactionController tc = activation.getTransactionController();
            dcoci = tc.getDynamicCompiledConglomInfo(conglomId);
            // the saved objects, if it exists
            if (heapColRefItem != -1) {
                this.accessedHeapCols = (FormatableBitSet)saved[heapColRefItem];
            }
            if (allColRefItem != -1) {
                this.accessedAllCols = (FormatableBitSet)saved[allColRefItem];
            }

            // retrieve the array of columns coming from the index
            indexCols = ((ReferencedColumnsDescriptorImpl) saved[indexColMapItem]).getReferencedColumnPositions();
			/* Get the result row template */
            resultRow = generatedMethod.invoke();

            compactRow = operationInformation.compactRow(resultRow, accessedAllCols, false);

            int[] baseColumnMap = operationInformation.getBaseColumnMap();
            if(heapOnlyColRefItem!=-1){
                this.heapOnlyCols = (FormatableBitSet)saved[heapOnlyColRefItem];
                adjustedBaseColumnMap = new int[heapOnlyCols.getNumBitsSet()];
                int pos=0;
                for(int i=heapOnlyCols.anySetBit();i>=0;i=heapOnlyCols.anySetBit(i)){
                    adjustedBaseColumnMap[pos] = baseColumnMap[i];
                    pos++;
                }
            }

            if (accessedHeapCols == null) {
                rowArray = resultRow.getRowArray();

            }
            else {
                // Figure out how many columns are coming from the heap
                final DataValueDescriptor[] resultRowArray = resultRow.getRowArray();
                final int heapOnlyLen = heapOnlyCols.getLength();

                // Need a separate DataValueDescriptor array in this case
                rowArray = new DataValueDescriptor[heapOnlyLen];
                final int minLen = Math.min(resultRowArray.length, heapOnlyLen);

                // Make a copy of the relevant part of rowArray
                for (int i = 0; i < minLen; ++i) {
                    if (resultRowArray[i] != null && heapOnlyCols.isSet(i)) {
                        rowArray[i] = resultRowArray[i];
                    }
                }
                if (indexCols != null) {
                    for (int index = 0; index < indexCols.length; index++) {
                        if (indexCols[index] != -1) {
                            compactRow.setColumn(index + 1,source.getExecRowDefinition().getColumn(indexCols[index] + 1));
                        }
                    }
                }
            }
            SpliceLogUtils.trace(LOG,"accessedAllCols=%s,accessedHeapCols=%s,heapOnlyCols=%s,accessedCols=%s",accessedAllCols,accessedHeapCols,heapOnlyCols,accessedCols);
            SpliceLogUtils.trace(LOG,"rowArray=%s,compactRow=%s,resultRow=%s,resultSetNumber=%d",
                    Arrays.asList(rowArray),compactRow,resultRow,resultSetNumber);
            getConglomerate();
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!",e);
        }
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return this.source;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        SpliceLogUtils.trace(LOG,"getSubOperations");
        return Collections.singletonList(source);
    }

    private SpliceConglomerate getConglomerate() throws StandardException{
        if(conglomerate==null)
            conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(conglomId);
        return conglomerate;
    }

    private int[] getColumnOrdering() throws StandardException{
        if (columnOrdering == null)
            columnOrdering = getConglomerate().getColumnOrdering();
        return columnOrdering;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if(readerBuilder==null){
            SConfiguration configuration=EngineDriver.driver().getConfiguration();
            int indexBatchSize = configuration.getIndexBatchSize();
            int lookupBlocks = configuration.getIndexLookupBlocks();
            readerBuilder = new IndexRowReaderBuilder()
                    .mainTableConglomId(conglomId)
                    .outputTemplate(compactRow)
                    .transaction(getCurrentTransaction())
                    .indexColumns(indexCols)
                    .mainTableKeyColumnEncodingOrder(getColumnOrdering())
                    .mainTableKeyColumnTypes(getKeyColumnTypes())
                    .mainTableKeyColumnSortOrder(getConglomerate().getAscDescInfo())
                    .mainTableKeyDecodingMap(getMainTableKeyDecodingMap())
                    .mainTableAccessedKeyColumns(getMainTableAccessedKeyColumns())
                    .mainTableVersion(tableVersion)
                    .mainTableRowDecodingMap(operationInformation.getBaseColumnMap())
                    .mainTableAccessedRowColumns(getMainTableRowColumns())
                    .numConcurrentLookups((getEstimatedRowCount()>2*indexBatchSize?lookupBlocks:-1))
                    .lookupBatchSize(indexBatchSize);
        }
        OperationContext context = dsp.createOperationContext(this);
        readerBuilder.transaction(context.getTxn());
        return source.getDataSet(dsp)
            .mapPartitions(new IndexToBaseRowFlatMapFunction(context,readerBuilder), false, true, "Fetch Base Rows")
            .filter(new IndexToBaseRowFilterPredicateFunction(context), true, true, "Apply Filter");
    }

    private FormatableBitSet getMainTableAccessedKeyColumns() throws StandardException {
        int[] keyColumnEncodingOrder = getColumnOrdering();
        FormatableBitSet accessedKeys = new FormatableBitSet(keyColumnEncodingOrder.length);
        for(int i=0;i<keyColumnEncodingOrder.length;i++){
            int keyColumn = keyColumnEncodingOrder[i];
            if(heapOnlyCols.get(keyColumn))
                accessedKeys.set(i);
        }
        return accessedKeys;
    }

    private FormatableBitSet getMainTableRowColumns() throws StandardException {
        int[] keyColumnEncodingOrder = getColumnOrdering();
        FormatableBitSet accessedKeys = new FormatableBitSet(heapOnlyCols);
        for(int i=0;i<keyColumnEncodingOrder.length;i++){
            int keyColumn = keyColumnEncodingOrder[i];
            if(heapOnlyCols.get(keyColumn))
                accessedKeys.clear(keyColumn);
        }
        return accessedKeys;
    }

    private int[] getKeyColumnTypes() throws StandardException {
        int[] keyColumnEncodingOrder = getColumnOrdering();
        if(keyColumnEncodingOrder==null) return null; //no keys to worry about
        int[] allFormatIds = getConglomerate().getFormat_ids();
        int[] keyFormatIds = new int[keyColumnEncodingOrder.length];
        for(int i=0,pos=0;i<keyColumnEncodingOrder.length;i++){
            int keyColumnPosition = keyColumnEncodingOrder[i];
            if(keyColumnPosition>=0){
                keyFormatIds[pos] = allFormatIds[keyColumnPosition];
                pos++;
            }
        }
        return keyFormatIds;
    }

    private int[] getMainTableKeyDecodingMap() throws StandardException {
        FormatableBitSet keyCols = getMainTableAccessedKeyColumns();

        int[] keyColumnEncodingOrder = getColumnOrdering();
        int[] baseColumnMap = operationInformation.getBaseColumnMap();

        int[] kDecoderMap = new int[keyColumnEncodingOrder.length];
        Arrays.fill(kDecoderMap, -1);
        for(int i=0;i<keyColumnEncodingOrder.length;i++){
            int baseKeyColumnPosition = keyColumnEncodingOrder[i]; //the position of the column in the base row
            if(keyCols.get(i)){
                kDecoderMap[i] = baseColumnMap[baseKeyColumnPosition];
                baseColumnMap[baseKeyColumnPosition] = -1;
            }else
                kDecoderMap[i] = -1;
        }

        return kDecoderMap;
    }

    @Override
    public ExecRow getExecRowDefinition() {
        return compactRow.getClone();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    public String getIndexName() {
        return this.indexName;
    }

    public  FormatableBitSet getAccessedHeapCols() {
        return this.accessedHeapCols;
    }

    public SpliceOperation getSource() {
        return this.source;
    }

    @Override
    public String toString() {
        return String.format("IndexRowToBaseRow {source=%s,indexName=%s,conglomId=%d,resultSetNumber=%d}",
                source,indexName,conglomId,resultSetNumber);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("IndexRowToBaseRow:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("accessedCols:").append(accessedCols)
                .append(indent).append("resultRowAllocatorMethodName:").append(resultRowAllocatorMethodName)
                .append(indent).append("indexName:").append(indexName)
                .append(indent).append("accessedHeapCols:").append(accessedHeapCols)
                .append(indent).append("heapOnlyCols:").append(heapOnlyCols)
                .append(indent).append("accessedAllCols:").append(accessedAllCols)
                .append(indent).append("indexCols:").append(Arrays.toString(indexCols))
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }


    @Override
    public int[] getAccessedNonPkColumns() throws StandardException{
        int[] baseColumnMap = operationInformation.getBaseColumnMap();
        int[] nonPkCols = new int[baseColumnMap.length];
        System.arraycopy(baseColumnMap, 0, nonPkCols, 0, nonPkCols.length);
        for (int col:getColumnOrdering()){
            if (col < nonPkCols.length) {
                nonPkCols[col] = -1;
            }
        }
        return nonPkCols;
    }

    public Restriction getRestriction() {
        Restriction mergeRestriction = Restriction.noOpRestriction;
        if (restriction != null) {
            mergeRestriction = new Restriction() {
                @Override
                public boolean apply(ExecRow row) throws StandardException {
                    activation.setCurrentRow(row, resultSetNumber);
                    DataValueDescriptor shouldKeep = restriction.invoke();
                    return !shouldKeep.isNull() && shouldKeep.getBoolean();
                }
            };
        }
        return mergeRestriction;
    }

    public String getScopeName() {
        return "Index Base Row Lookup";
    }

    @Override
    public ExecIndexRow getStartPosition() throws StandardException {
        return source.getStartPosition();
    }
}
