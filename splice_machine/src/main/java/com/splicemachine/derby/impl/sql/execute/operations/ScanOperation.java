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

import splice.com.google.common.base.Strings;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.DataScan;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

public abstract class ScanOperation extends SpliceBaseOperation{
    private static final Logger LOG=Logger.getLogger(ScanOperation.class);
    private static final long serialVersionUID=7l;
    public int lockMode;
    public int isolationLevel;
    protected boolean oneRowScan;
    protected ScanInformation<ExecRow> scanInformation;
    protected String tableName;
    protected String tableDisplayName;
    protected String indexName;
    public boolean isConstraint;
    public boolean forUpdate;
    protected ExecRow currentTemplate;
    protected int[] columnOrdering;
    protected int[] getColumnOrdering;
    protected int[] keyDecodingMap;
    protected int[] rowDecodingMap;
    protected String scanQualifiersField;
    protected String tableVersion;
    protected boolean rowIdKey;
    protected boolean pin;
    protected int splits;
    protected String delimited;
    protected String escaped;
    protected String lines;
    protected String storedAs;
    protected String location;
    int partitionRefItem;
    protected int[] partitionColumnMap;
    protected ExecRow defaultRow;
    public static final int SCAN_CACHE_SIZE = 1000;

    public ScanOperation(){
        super();
    }

    public ScanOperation(long conglomId,Activation activation,int resultSetNumber,
                         GeneratedMethod startKeyGetter,int startSearchOperator,
                         GeneratedMethod stopKeyGetter,int stopSearchOperator,
                         boolean sameStartStopPosition,
                         boolean rowIdKey,
                         String scanQualifiersField,
                         GeneratedMethod resultRowAllocator,
                         int lockMode,boolean tableLocked,int isolationLevel,
                         int colRefItem,
                         int indexColItem,
                         boolean oneRowScan,
                         double optimizerEstimatedRowCount,
                         double optimizerEstimatedCost,String tableVersion,
                         boolean pin, int splits, String delimited, String escaped, String lines,
                         String storedAs, String location, int partitionRefItem, GeneratedMethod defaultRowFunc, int defaultValueMapItem

    ) throws StandardException{
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.lockMode=lockMode;
        this.isolationLevel=isolationLevel;
        this.oneRowScan=oneRowScan;
        this.scanQualifiersField=scanQualifiersField;
        this.tableVersion=tableVersion;
        this.rowIdKey = rowIdKey;
        this.pin = pin;
        this.splits = splits;
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.partitionRefItem = partitionRefItem;
        this.scanInformation=new DerbyScanInformation(resultRowAllocator.getMethodName(),
                startKeyGetter!=null?startKeyGetter.getMethodName():null,
                stopKeyGetter!=null?stopKeyGetter.getMethodName():null,
                scanQualifiersField!=null?scanQualifiersField:null,
                conglomId,
                colRefItem,
                indexColItem,
                sameStartStopPosition,
                startSearchOperator,
                stopSearchOperator,
                rowIdKey,
                tableVersion,
                defaultRowFunc!=null?defaultRowFunc.getMethodName():null,
                defaultValueMapItem
        );
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
    public int[] getColumnOrdering() throws StandardException{
        if(columnOrdering==null){
            columnOrdering=scanInformation.getColumnOrdering();
        }
        return columnOrdering;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        oneRowScan=in.readBoolean();
        lockMode=in.readInt();
        isolationLevel=in.readInt();
        scanInformation=(ScanInformation<ExecRow>)in.readObject();
        tableVersion=in.readUTF();
        rowIdKey = in.readBoolean();
        pin = in.readBoolean();
        delimited = in.readBoolean()?in.readUTF():null;
        escaped = in.readBoolean()?in.readUTF():null;
        lines = in.readBoolean()?in.readUTF():null;
        storedAs = in.readBoolean()?in.readUTF():null;
        location = in.readBoolean()?in.readUTF():null;
        partitionRefItem = in.readInt();
        splits = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeBoolean(oneRowScan);
        out.writeInt(lockMode);
        out.writeInt(isolationLevel);
        out.writeObject(scanInformation);
        out.writeUTF(tableVersion);
        out.writeBoolean(rowIdKey);
        out.writeBoolean(pin);
        out.writeBoolean(delimited!=null);
        if (delimited!=null)
            out.writeUTF(delimited);
        out.writeBoolean(escaped!=null);
        if (escaped!=null)
            out.writeUTF(escaped);
        out.writeBoolean(lines!=null);
        if (lines!=null)
            out.writeUTF(lines);
        out.writeBoolean(storedAs!=null);
        if (storedAs!=null)
            out.writeUTF(storedAs);
        out.writeBoolean(location!=null);
        if (location!=null)
            out.writeUTF(location);
        out.writeInt(partitionRefItem);
        out.writeInt(splits);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"init called");
        super.init(context);
        scanInformation.initialize(context);
        try{
            GenericStorablePreparedStatement statement = context.getPreparedStatement();
            ExecRow candidate=scanInformation.getResultRow();
            currentRow=operationInformation.compactRow(candidate,scanInformation);
            currentTemplate=currentRow.getClone();
            if(currentRowLocation==null)
                currentRowLocation=new HBaseRowLocation();
            if (this.partitionRefItem == -1)
                partitionColumnMap = DerbyScanInformation.Empty_Array;
            else
                partitionColumnMap = ((ReferencedColumnsDescriptorImpl) statement.getSavedObject(partitionRefItem)).getReferencedColumnPositions();
            defaultRow = scanInformation.getDefaultRow();
        }catch(Exception e){
            SpliceLogUtils.logAndThrowRuntime(LOG,"Operation Init Failed!",e);
        }
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return null;
    }

    protected void initIsolationLevel(){
        SpliceLogUtils.trace(LOG, "initIsolationLevel");
    }

    public DataScan getNonSIScan() throws StandardException{
        /*
		 * Intended to get a scan which does NOT set up SI underneath us (since
		 * we are doing it ourselves).
		 */
        DataScan s=getScan();

        if(oneRowScan){
            /*
             * Limit the cache and batch size for performance. The underlying architecture
             * may also choose to do further optimizations under the hood based on its own
             * internal logic.
             */
            s = s.cacheRows(2).batchCells(-1);
        }
        // Makes it a small scan for 100 rows of fewer
        // Bug where splits return 1 extra row
//        else if (this.getEstimatedRowCount()<100) {
//            s = s.cacheRows(100).batchCells(-1);
//        } else {
            s.cacheRows(SCAN_CACHE_SIZE).batchCells(-1);
//        }
        deSiify(s);
        return s;
    }

    public DataScan getReversedNonSIScan() throws StandardException{
        return getNonSIScan().reverseOrder();
    }

    /**
     * Get the Stored format ids for the columns in the key. The returned int[] is ordered
     * by the encoding order of the keys.
     *
     * @return the format ids for the columns in the key.
     * @throws StandardException
     */
    protected int[] getKeyFormatIds() throws StandardException{
        return getKeyFormatIds(scanInformation.getColumnOrdering(),scanInformation.getConglomerate().getFormat_ids());
    }

    public static int[] getKeyFormatIds(int[] keyColumnEncodingOrder, int[] formatIds) throws StandardException {
        if(keyColumnEncodingOrder==null) return null; //no keys to worry about
        int[] keyFormatIds=new int[keyColumnEncodingOrder.length];
        for(int i=0, pos=0;i<keyColumnEncodingOrder.length;i++){
            int keyColumnPosition=keyColumnEncodingOrder[i];
            if(keyColumnPosition>=0){
                keyFormatIds[pos]=formatIds[keyColumnPosition];
                pos++;
            }
        }
        return keyFormatIds;
    }

    /**
     * @return a map from the accessed (desired) key columns to their position in the decoded row.
     * @throws StandardException
     */

    public static int[] getKeyDecodingMap(FormatableBitSet accessedPKColumns,
                                             int[] keyColumnEncodingOrder,
                                             int[] baseColumnMap) throws StandardException {

        int[] kDecoderMap=new int[keyColumnEncodingOrder.length];
        Arrays.fill(kDecoderMap,-1);
        for(int i=0;i<keyColumnEncodingOrder.length;i++){
            int baseKeyColumnPosition=keyColumnEncodingOrder[i]; //the position of the column in the base row
            if(accessedPKColumns.get(i)){
                kDecoderMap[i]=baseColumnMap[baseKeyColumnPosition];
            }else
                kDecoderMap[i]=-1;
        }
        return kDecoderMap;
    }

    public static int[] getRowDecodingMap(FormatableBitSet accessedPKColumns,
                                          int[] keyColumnEncodingOrder,
                                          int[] baseColumnMap) throws StandardException {

        int[] rowDecodingMap=baseColumnMap.clone();
        for(int i=0;i<keyColumnEncodingOrder.length;i++){
            int baseKeyColumnPosition=keyColumnEncodingOrder[i]; //the position of the column in the base row
            if(accessedPKColumns.get(i))
                rowDecodingMap[baseKeyColumnPosition]=-1;
        }
        return rowDecodingMap;
    }


    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
    public int[] getKeyDecodingMap() throws StandardException{
        if(keyDecodingMap==null) {
            keyDecodingMap = getKeyDecodingMap(
                    scanInformation.getAccessedPkColumns(),
                    scanInformation.getColumnOrdering(),
                    operationInformation.getBaseColumnMap());
        }
        return keyDecodingMap;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
    public int[] getRowDecodingMap() throws StandardException {
        if(rowDecodingMap==null) {
            rowDecodingMap = getRowDecodingMap(
                    scanInformation.getAccessedPkColumns(),
                    scanInformation.getColumnOrdering(),
                    operationInformation.getBaseColumnMap());
        }
        return rowDecodingMap;

    }

    /**
     * Remove SI-specific behaviors from the scan, so that we can handle it ourselves correctly.
     */
    public static void deSiify(DataScan scan){
        //exclude this from SI treatment, since we're doing it internally
        scan.addAttribute(SIConstants.SI_NEEDED,null);
        scan.returnAllVersions();
    }

    protected DataScan getScan() throws StandardException{

        return scanInformation.getScan(getCurrentTransaction(),
                ((BaseActivation)activation).getScanStartOverride(),getKeyDecodingMap(),
                ((BaseActivation)activation).getScanStopOverride());
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber){
        return operationInformation.getBaseColumnMap();
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return tableName.equals(String.valueOf(tableNumber));
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getTableDisplayName() {
        return this.tableDisplayName;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getIndexDisplayName() {
        // for now returns indexName (which is a readable string)
        // but this hook leaves flexibility for later
        return this.indexName;
    }

    public boolean isIndexScan() {
        return this.indexName != null;
    }

    @Override
    public String prettyPrint(int indentLevel){
        String indent="\n"+ Strings.repeat("\t", indentLevel);
        return "Scan:"
                +indent +"resultSetNumber:"+resultSetNumber
                +indent+"optimizerEstimatedCost:"+optimizerEstimatedCost+","
                +indent+"optimizerEstimatedRowCount:"+optimizerEstimatedRowCount+","
                +indent+"scanInformation:"+scanInformation
                +indent+"tableName:"+tableName;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getKeyColumns(){
        return columnOrdering;
    }

    public boolean[] getAscDescInfo() throws StandardException {
        return scanInformation.getConglomerate().getAscDescInfo();
    }

    @Override
    public ExecIndexRow getStartPosition() throws StandardException{
        return scanInformation.getStartPosition();
    }

    public String getTableVersion(){
        return tableVersion;
    }

    public String getScopeName() {
        StringBuilder sb = new StringBuilder();
        sb.append(getScopeBaseOpName());
        if (isIndexScan()) {
            sb.append(" Index ").append(getIndexDisplayName());
            sb.append(" (Table ").append(getTableDisplayName()).append(")");
        } else {
            sb.append(" Table ").append(getTableDisplayName());
        }

        return sb.toString();
    }

    protected String getScopeBaseOpName() {
        return super.getScopeName();
    }

    @Override
    public ScanInformation<ExecRow> getScanInformation() {
        return scanInformation;
    }

    /**
     *
     * Hack until we figure out rowid qualifiers.
     *
     * @return
     */
    public boolean getRowIdKey() {
        return rowIdKey;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
    public int[] getPartitionColumnMap() {
        return partitionColumnMap;
    }

    public String getStoredAs() {
        return storedAs;
    }

    public int getSplits() {
        return splits;
    }

    @Override
    public FormatableBitSet getAccessedColumns() throws StandardException{
        return scanInformation.getAccessedColumns();
    }
}
