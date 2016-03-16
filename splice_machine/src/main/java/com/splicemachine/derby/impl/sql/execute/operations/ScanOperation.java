package com.splicemachine.derby.impl.sql.execute.operations;

import org.sparkproject.guava.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
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
    protected String scanQualifiersField;
    protected String tableVersion;

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
                         double optimizerEstimatedCost,String tableVersion) throws StandardException{
        super(activation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
        this.lockMode=lockMode;
        this.isolationLevel=isolationLevel;
        this.oneRowScan=oneRowScan;
        this.scanQualifiersField=scanQualifiersField;
        this.tableVersion=tableVersion;
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
                tableVersion
        );
    }

    protected int[] getColumnOrdering() throws StandardException{
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
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeBoolean(oneRowScan);
        out.writeInt(lockMode);
        out.writeInt(isolationLevel);
        out.writeObject(scanInformation);
        out.writeUTF(tableVersion);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"init called");
        super.init(context);
        scanInformation.initialize(context);
        try{
            ExecRow candidate=scanInformation.getResultRow();
            currentRow=operationInformation.compactRow(candidate,scanInformation);
            currentTemplate=currentRow.getClone();
            if(currentRowLocation==null)
                currentRowLocation=new HBaseRowLocation();
        }catch(Exception e){
            SpliceLogUtils.logAndThrowRuntime(LOG,"Operation Init Failed!",e);
        }
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return null;
    }

    protected void initIsolationLevel(){
        SpliceLogUtils.trace(LOG,"initIsolationLevel");
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
//            scan.setSmall(true);
//            scan.setCaching(2); // Limit the batch size for performance
//            // Setting caching to 2 instead of 1 removes an extra RPC during Single Row Result Scans
        }
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
        int[] keyColumnEncodingOrder=scanInformation.getColumnOrdering();
        if(keyColumnEncodingOrder==null) return null; //no keys to worry about
        int[] allFormatIds=scanInformation.getConglomerate().getFormat_ids();
        int[] keyFormatIds=new int[keyColumnEncodingOrder.length];
        for(int i=0, pos=0;i<keyColumnEncodingOrder.length;i++){
            int keyColumnPosition=keyColumnEncodingOrder[i];
            if(keyColumnPosition>=0){
                keyFormatIds[pos]=allFormatIds[keyColumnPosition];
                pos++;
            }
        }
        return keyFormatIds;
    }

    /**
     * @return a map from the accessed (desired) key columns to their position in the decoded row.
     * @throws StandardException
     */
    protected int[] getKeyDecodingMap() throws StandardException{
        if(keyDecodingMap==null){
            FormatableBitSet pkCols=scanInformation.getAccessedPkColumns();

            int[] keyColumnEncodingOrder=scanInformation.getColumnOrdering();
            int[] baseColumnMap=operationInformation.getBaseColumnMap();

            int[] kDecoderMap=new int[keyColumnEncodingOrder.length];
            Arrays.fill(kDecoderMap,-1);
            for(int i=0;i<keyColumnEncodingOrder.length;i++){
                int baseKeyColumnPosition=keyColumnEncodingOrder[i]; //the position of the column in the base row
                if(pkCols.get(i)){
                    kDecoderMap[i]=baseColumnMap[baseKeyColumnPosition];
                    baseColumnMap[baseKeyColumnPosition]=-1;
                }else
                    kDecoderMap[i]=-1;
            }


            keyDecodingMap=kDecoderMap;
        }
        return keyDecodingMap;
    }


    protected void deSiify(DataScan scan){
				/*
				 * Remove SI-specific behaviors from the scan, so that we can handle it ourselves correctly.
				 */
        //exclude this from SI treatment, since we're doing it internally
        scan.addAttribute(SIConstants.SI_NEEDED,null);
        scan.returnAllVersions();

//        Map<byte[], NavigableSet<byte[]>> familyMap=scan.getFamilyMap();
//        if(familyMap!=null){
//            NavigableSet<byte[]> bytes=familyMap.get(SpliceConstants.DEFAULT_FAMILY_BYTES);
//            if(bytes!=null)
//                bytes.clear(); //make sure we get all columns
//        }
    }

    protected DataScan getScan() throws StandardException{
        return scanInformation.getScan(getCurrentTransaction(),
                ((BaseActivation)activation).getScanStartOverride(),getKeyDecodingMap(),null,((BaseActivation)activation).getScanStopOverride());
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
        String indent="\n"+Strings.repeat("\t",indentLevel);
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
}
