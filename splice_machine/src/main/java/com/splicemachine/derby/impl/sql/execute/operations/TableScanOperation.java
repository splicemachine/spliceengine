package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMInputFormat;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.UUIDGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TableScanOperation extends ScanOperation{
    protected static final DerbyFactory derbyFactory=DerbyFactoryDriver.derbyFactory;
    private static final long serialVersionUID=3l;
    private static Logger LOG=Logger.getLogger(TableScanOperation.class);
    private static List<NodeType> NODE_TYPES=Arrays.asList(NodeType.MAP,NodeType.SCAN);
    protected int indexColItem;
    public String userSuppliedOptimizerOverrides;
    public int rowsPerRead;
    protected boolean runTimeStatisticsOn;
    private Properties scanProperties;
    public String startPositionString;
    public String stopPositionString;
    public ByteSlice slice;
    private int[] baseColumnMap;
    private SITableScanner tableScanner;
    protected static final String NAME=TableScanOperation.class.getSimpleName().replaceAll("Operation","");
    protected byte[] tableNameBytes;

    @Override
    public String getName(){
        return NAME;
    }


    public TableScanOperation(){
        super();
    }

    public TableScanOperation(ScanInformation scanInformation,
                              OperationInformation operationInformation,
                              int lockMode,
                              int isolationLevel) throws StandardException{
        super(scanInformation,operationInformation,lockMode,isolationLevel);
    }

    @SuppressWarnings("UnusedParameters")
    public TableScanOperation(long conglomId,
                              StaticCompiledOpenConglomInfo scoci,
                              Activation activation,
                              GeneratedMethod resultRowAllocator,
                              int resultSetNumber,
                              GeneratedMethod startKeyGetter,int startSearchOperator,
                              GeneratedMethod stopKeyGetter,int stopSearchOperator,
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
                              double optimizerEstimatedCost) throws StandardException{
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,rowIdKey,qualifiersField,resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,indexColItem,oneRowScan,optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG,"instantiated for tablename %s or indexName %s with conglomerateID %d",
                tableName,indexName,conglomId);
        this.forUpdate=forUpdate;
        this.isConstraint=isConstraint;
        this.rowsPerRead=rowsPerRead;
        this.tableName=Long.toString(scanInformation.getConglomerateId());
        this.tableNameBytes=Bytes.toBytes(this.tableName);
        this.indexColItem=indexColItem;
        this.indexName=indexName;
        runTimeStatisticsOn=operationInformation.isRuntimeStatisticsEnabled();
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"statisticsTimingOn=%s,isTopResultSet=%s,runTimeStatisticsOn%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f",statisticsTimingOn,isTopResultSet,runTimeStatisticsOn,optimizerEstimatedCost,optimizerEstimatedRowCount);
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        tableName=in.readUTF();
        tableNameBytes=Bytes.toBytes(tableName);
        indexColItem=in.readInt();
        if(in.readBoolean())
            indexName=in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeInt(indexColItem);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        this.baseColumnMap=operationInformation.getBaseColumnMap();
        this.slice=ByteSlice.empty();
        this.startExecutionTime=System.currentTimeMillis();
        this.scan=context.getScan();

        //start reading rows
        if(regionScanner!=null)
            regionScanner.start();

        this.txnRegion=context.getTransactionalRegion();

        if(activation.isTraced()){
            String tableNameInfo=null;
            if(this.indexName!=null){
                tableNameInfo="index:"+indexName+")";
            }else if(this.tableName!=null){
                String tname=scanInformation.getTableName();
                tableNameInfo="table:"+(tname==null?tableName:tname);
            }
            if(info==null)
                info=tableNameInfo;
            else if(tableNameInfo!=null && !info.contains(tableNameInfo))
                info+=", "+tableNameInfo;
        }
    }


    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.emptyList();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"getMapRowProvider");
        beginTime=System.currentTimeMillis();
        if(scan==null || !scanSet){
            scan=getNonSIScan(spliceRuntimeContext);
        }

        SpliceUtils.setInstructions(scan,activation,top,spliceRuntimeContext);
        ClientScanProvider provider = new ClientScanProvider("tableScan",Bytes.toBytes(tableName),scan,decoder,spliceRuntimeContext);
        nextTime+=System.currentTimeMillis()-beginTime;
        return provider;
    }


    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext,boolean returnDefaultValue) throws StandardException, IOException{
        return getMapRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{

        final UUIDGenerator generator=SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
        DataHash hash=new SuppliedDataHash(new StandardSupplier<byte[]>(){
            @Override
            public byte[] get() throws StandardException{
                return generator.nextBytes();
            }
        });

        return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
        columnOrdering=scanInformation.getColumnOrdering();
        ExecRow defnRow=getExecRowDefinition();
        DescriptorSerializer[] serializers=VersionedSerializers.latestVersion(false).getSerializers(defnRow);
        return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null,serializers);
    }

    @Override
    public List<NodeType> getNodeTypes(){
        return NODE_TYPES;
    }

    @Override
    public ExecRow getExecRowDefinition(){
        return currentTemplate;
    }

    @Override
    public String prettyPrint(int indentLevel){
        return "Table"+super.prettyPrint(indentLevel);
    }

    @Override
    protected int getNumMetrics(){
        return 5;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats){
        if(tableScanner!=null){
            stats.addMetric(OperationMetric.FILTERED_ROWS,tableScanner.getRowsFiltered());
            stats.addMetric(OperationMetric.OUTPUT_ROWS,tableScanner.getRowsVisited()-tableScanner.getRowsFiltered());
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,tableScanner.getRowsVisited());
            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,tableScanner.getBytesVisited());
            TimeView time=tableScanner.getTime();
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,time.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,time.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,time.getUserTime());
        }
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        if(tableScanner==null){
            tableScanner=new TableScannerBuilder()
                    .scanner(regionScanner)
                    .region(txnRegion)
                    .transaction(operationInformation.getTransaction())
                    .metricFactory(spliceRuntimeContext)
                    .scan(scan)
                    .template(currentRow)
                    .tableVersion(scanInformation.getTableVersion())
                    .indexName(indexName)
                    .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                    .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                    .keyColumnTypes(getKeyFormatIds())
                    .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                    .keyDecodingMap(getKeyDecodingMap())
                    .rowDecodingMap(baseColumnMap).build();
            timer=spliceRuntimeContext.newTimer();
        }

        timer.startTiming();
        currentRow=tableScanner.next(spliceRuntimeContext);
        if(currentRow!=null){
            timer.tick(1);
            setCurrentRow(currentRow);
            setCurrentRowLocation(tableScanner.getCurrentRowLocation());
        }else{
            timer.stopTiming();
            clearCurrentRow();
            stopExecutionTime=System.currentTimeMillis();
            currentRowLocation=null;
        }

        return currentRow;
    }


    protected void setRowLocation(KeyValue sampleKv) throws StandardException{
        if(indexName!=null && currentRow.nColumns()>0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId()==StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
                     /*
						* If indexName !=null, then we are currently scanning an index,
						* so our RowLocation should point to the main table, and not to the
						* index (that we're actually scanning)
						*/
            currentRowLocation=(RowLocation)currentRow.getColumn(currentRow.nColumns());
        }else{
            slice.set(sampleKv.getBuffer(),sampleKv.getRowOffset(),sampleKv.getRowLength());
            currentRowLocation.setValue(slice);
        }
    }

    @Override
    public String toString(){
        try{
            return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,scanInformation.isKeyed(),resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }catch(Exception e){
            return String.format("TableScanOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,"UNKNOWN",resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }
    }

    @Override
    public void close() throws StandardException, IOException{
        if(rowDecoder!=null)
            rowDecoder.close();
        SpliceLogUtils.trace(LOG,"close in TableScan");
        beginTime=getCurrentTimeMillis();

        if(runTimeStatisticsOn){
            // This is where we get the scan properties for a subquery
            scanProperties=getScanProperties();
            startPositionString=printStartPosition();
            stopPositionString=printStopPosition();
        }

        if(forUpdate && scanInformation.isKeyed()){
            activation.clearIndexScanInfo();
        }

        super.close();

        closeTime+=getElapsedMillis(beginTime);
    }

    public Properties getScanProperties(){
        if(scanProperties==null)
            scanProperties=new Properties();
        scanProperties.setProperty("numPagesVisited","0");
        scanProperties.setProperty("numRowsVisited","0");
        scanProperties.setProperty("numRowsQualified","0");
        scanProperties.setProperty("numColumnsFetched","0");//FIXME: need to loop through accessedCols to figure out
        try{
            scanProperties.setProperty("columnsFetchedBitSet",""+scanInformation.getAccessedColumns());
        }catch(StandardException e){
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        //treeHeight

        return scanProperties;
    }


    @Override
    public int[] getAccessedNonPkColumns() throws StandardException{
        FormatableBitSet accessedNonPkColumns=scanInformation.getAccessedNonPkColumns();
        int num=accessedNonPkColumns.getNumBitsSet();
        int[] cols=null;
        if(num>0){
            cols=new int[num];
            int pos=0;
            for(int i=accessedNonPkColumns.anySetBit();i!=-1;i=accessedNonPkColumns.anySetBit(i)){
                cols[pos++]=baseColumnMap[i];
            }
        }
        return cols;
    }

    @Override
    public boolean providesRDD(){
        return true;
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext,SpliceOperation top) throws StandardException{
        assert currentTemplate!=null:"Current Template Cannot Be Null";
        int[] execRowTypeFormatIds=new int[currentTemplate.nColumns()];
        for(int i=0;i<currentTemplate.nColumns();i++){
            execRowTypeFormatIds[i]=currentTemplate.getColumn(i+1).getTypeFormatId();
        }
        TableScannerBuilder tsb=new TableScannerBuilder()
                .transaction(operationInformation.getTransaction())
                .scan(getNonSIScan(spliceRuntimeContext))
                .template(currentRow)
                .tableVersion(scanInformation.getTableVersion())
                .indexName(indexName)
                .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                .keyColumnTypes(getKeyFormatIds())
                .execRowTypeFormatIds(execRowTypeFormatIds)
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(baseColumnMap);
        JavaSparkContext ctx=SpliceSpark.getContext();
        Configuration conf=new Configuration(SIConstants.config);
        conf.set(MRConstants.SPLICE_CONGLOMERATE,tableName);
        conf.set(MRConstants.SPLICE_JDBC_STR,"jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin");
        try{
            conf.set(MRConstants.SPLICE_SCAN_INFO,tsb.getTableScannerBuilderBase64String());
        }catch(IOException ioe){
            throw StandardException.unexpectedUserException(ioe);
        }

        JavaPairRDD<RowLocation, ExecRow> rawRDD=ctx.newAPIHadoopRDD(conf,SMInputFormat.class,
                RowLocation.class,ExecRow.class);
        return rawRDD.map(new Function<Tuple2<RowLocation, ExecRow>, LocatedRow>(){
            @Override
            public LocatedRow call(Tuple2<RowLocation, ExecRow> tuple) throws Exception{
                return new LocatedRow(tuple._1(),tuple._2());
            }
        });
    }

}
