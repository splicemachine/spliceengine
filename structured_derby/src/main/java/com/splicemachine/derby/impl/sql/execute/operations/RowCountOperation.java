package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.AbstractScanProvider;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.table.SpliceHTableUtil;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 5/15/13
 */
public class RowCountOperation extends SpliceBaseOperation{
    private static final long serialVersionUID = 1l;
    private static final Logger LOG = Logger.getLogger(RowCountOperation.class);
    private static final List<NodeType> nodeTypes = Arrays.asList(NodeType.SCAN);

    private static final byte[] OFFSET_RESULTS_COL = Encoding.encode(-1000);

    private String offsetMethodName;
    private String fetchFirstMethodName;

    private GeneratedMethod offsetMethod;
    private GeneratedMethod fetchFirstMethod;
    private boolean hasJDBClimitClause;

    private SpliceOperation source;

    private int numColumns;
    private long offset;
    private long fetchFirst;

    private boolean runTimeStatsOn;

    private boolean firstTime;

    private long rowsFetched;
    private Scan regionScan;
    private SpliceOperationRegionScanner spliceScanner;

    private long rowsSkipped;

    public RowCountOperation() {
    }

    public RowCountOperation(SpliceOperation source,
                             Activation activation,
                             int resultSetNumber,
                             GeneratedMethod offsetMethod,
                             GeneratedMethod fetchFirstMethod,
                             boolean hasJDBClimitClause,
                             double optimizerEstimatedRowCount,
                             double optimizerEstimatedCost) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.offsetMethod = offsetMethod;
        this.fetchFirstMethod = fetchFirstMethod;
        this.offsetMethodName = (offsetMethod==null)? null: offsetMethod.getMethodName();
        this.fetchFirstMethodName = (fetchFirstMethod==null)? null: fetchFirstMethod.getMethodName();
        this.hasJDBClimitClause = hasJDBClimitClause;
        this.source = source;
        firstTime = true;
        rowsFetched =0;
        runTimeStatsOn = activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
        init(SpliceOperationContext.newContext(activation));

        offset = getTotalOffset();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation)in.readObject();
        offsetMethodName = readNullableString(in);
        fetchFirstMethodName = readNullableString(in);
        runTimeStatsOn = in.readBoolean();
        hasJDBClimitClause = in.readBoolean();
        rowsSkipped = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        writeNullableString(offsetMethodName,out);
        writeNullableString(fetchFirstMethodName,out);
        out.writeBoolean(runTimeStatsOn);
        out.writeBoolean(hasJDBClimitClause);
        out.writeLong(rowsSkipped);
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        source.open();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        source.init(context);
        GenericStorablePreparedStatement gsps = context.getPreparedStatement();
        if(offsetMethodName!=null)
            offsetMethod = gsps.getActivationClass().getMethod(offsetMethodName);
        if(fetchFirstMethodName!=null)
            fetchFirstMethod = gsps.getActivationClass().getMethod(fetchFirstMethodName);
        firstTime=true;
        rowsFetched=0;


        //determine our offset
        this.regionScan = context.getScan();
        this.spliceScanner = context.getSpliceRegionScanner();
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        ExecRow row;
        if(firstTime){
            firstTime=false;
            getTotalOffset();
            getFetchLimit();
        }

        if(fetchFirstMethod!=null && rowsFetched >=fetchFirst){
            row =null;
        }else{
            do{
                row = source.nextRow(spliceRuntimeContext);
                if(row!=null){
                    if(rowsSkipped<offset){
                        rowsSkipped++;
                        continue;
                    }
                    rowsFetched++;
                    rowsSeen++;
                    if(runTimeStatsOn){
                        if(!isTopResultSet){
                            StatementContext sc = activation.getLanguageConnectionContext().getStatementContext();
                            if(sc!=null)
                                subqueryTrackingArray = sc.getSubqueryTrackingArray();
                        }
                    }

                    setCurrentRow(row);
                    return row;
                }else if (rowsSkipped > 0) {
                    spliceScanner.addAdditionalColumnToReturn(OFFSET_RESULTS_COL,Bytes.toBytes(rowsSkipped));
                }
            }while(row!=null);
        }
        setCurrentRow(null);
        return null;
    }

    private long getTotalOffset() throws StandardException {
        if(offsetMethod!=null){
            DataValueDescriptor offVal = (DataValueDescriptor)offsetMethod.invoke(activation);
            if(offVal.isNotNull().getBoolean()){
                offset = offVal.getLong();
            }

        }

        return offset;
    }

    private long getFetchLimit() throws StandardException {
        if(fetchFirstMethod!=null){
            DataValueDescriptor fetchFirstVal = (DataValueDescriptor)fetchFirstMethod.invoke(activation);
            if(fetchFirstVal.isNotNull().getBoolean()){
                fetchFirst = fetchFirstVal.getLong();
            }
        }
        return fetchFirst;
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
    	SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        return new SpliceNoPutResultSet(activation,this,getReduceRowProvider(this,getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()),spliceRuntimeContext));
    }

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        ExecRow row = getExecRowDefinition();
        return RowEncoder.create(row.nColumns(),null,null,null, KeyType.BARE,RowMarshaller.packed());
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("RowCount:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("offsetMethodName:").append(offsetMethodName)
                .append(indent).append("fetchFirstMethodName:").append(fetchFirstMethodName)
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        RowProvider provider = source.getReduceRowProvider(top, decoder, spliceRuntimeContext);
        long fetchLimit = getFetchLimit();
        long offset = getTotalOffset();

        if(offset>0){
            if(provider instanceof AbstractScanProvider){
                AbstractScanProvider scanProvider = (AbstractScanProvider)provider;
                provider = OffsetScanRowProvider.create(scanProvider.getRowTemplate(),
                        top,
                        scanProvider.toScan(),
                        offset,
                        baseColumnMap,
                        scanProvider.getTableName(),
                        spliceRuntimeContext);
            }
        }else if(provider instanceof AbstractScanProvider){
            final AbstractScanProvider scanProvider = (AbstractScanProvider)provider;
            AbstractScanProvider newWrap = new AbstractScanProvider(scanProvider){
                @Override
                public Result getResult() throws StandardException, IOException {
                    Result result = scanProvider.getResult();
                    if(result == null || !result.containsColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY))
                        return null;
                    return result;
                }

                @Override
                public Scan toScan() {
                    return scanProvider.toScan();
                }

                @Override
                public void open() throws StandardException {
                    scanProvider.open();
                }

                @Override
                public byte[] getTableName() {
                    return scanProvider.getTableName();
                }

				@Override
				public SpliceRuntimeContext getSpliceRuntimeContext() {
					// TODO Auto-generated method stub
					return null;
				}
            };
            provider = newWrap;
        }

        if(fetchLimit > 0 &&fetchLimit < (long)Integer.MAX_VALUE){
            //set the caching size down if we only want to fetch back a few rows
            if(provider instanceof SingleScanRowProvider){
                int fetchSize = (int)fetchLimit;
                Scan scan = ((SingleScanRowProvider)provider).toScan();
                int caching = scan.getCaching();
                if(caching > fetchSize){
                    scan.setCaching(fetchSize);
                }
            }
        }
        if(fetchLimit>0)
            return new LimitedRowProvider(provider,fetchLimit);
        else
            return provider;
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
       return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return source.getExecRowDefinition();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    public SpliceOperation getSource() {
        return source;
    }

    public void setRowsSkipped(long rowsSkipped) {
        this.rowsSkipped = rowsSkipped;
    }

    public static class OffsetFilter extends FilterBase {
        private long offset;
        private long numSkipped =0;

        public OffsetFilter(){}

        public OffsetFilter(long offset){
            this.offset = offset;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(offset);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            offset = in.readLong();
        }

        @Override
        public boolean filterRowKey(byte[] buffer, int offset, int length) {
            boolean skip= numSkipped < this.offset;
            if(skip)
                numSkipped++;
            return skip;
        }
    }

    private static class OffsetScanRowProvider extends AbstractScanProvider {
        private final Scan fullScan;
        private Queue<Scan> offsetScans;
        private ResultScanner currentScan = null;
        private final long totalOffset;
        private long rowsSkipped;
        private byte[] tableName;
        private HTableInterface table;
        private SpliceOperation operation;

        private OffsetScanRowProvider(String type,
                                      SpliceOperation operation,
                                      RowDecoder rowDecoder,
                                      Scan fullScan,
                                      long totalOffset,
                                      byte[] tableName,
                                      SpliceRuntimeContext spliceRuntimeContext) {
            super(rowDecoder, type,spliceRuntimeContext);
            this.fullScan = fullScan;
            this.totalOffset = totalOffset;
            this.tableName = tableName;
            this.operation = operation;
        }

        public static OffsetScanRowProvider create(ExecRow rowTemplate,
                                                   SpliceOperation operation,
                                            Scan fullScan,
                                            long totalOffset,
                                            int[] baseColumnMap,
                                            byte[] tableName,
                                            SpliceRuntimeContext spliceRuntimeContext){
            RowDecoder rowDecoder = RowDecoder.create(rowTemplate,
                    null,null,null, RowMarshaller.packed(),baseColumnMap,false);

            return new OffsetScanRowProvider("offsetScan",operation,rowDecoder,fullScan,totalOffset,tableName, spliceRuntimeContext);
        }

        @Override
        public Result getResult() throws StandardException {
            if (currentScan == null) {
                Scan next = offsetScans.poll();

                if (next == null) return null; //we've finished

                //attach the rows to skip
                if(operation instanceof RowCountOperation)
                    ((RowCountOperation) operation).setRowsSkipped(rowsSkipped);
                else{
                    for (SpliceOperation op : operation.getSubOperations()) {
                        if (op instanceof RowCountOperation) {
                            ((RowCountOperation) op).setRowsSkipped(rowsSkipped);
                            break;
                        }
                    }
                }
                SpliceUtils.setInstructions(next,operation.getActivation(),operation, new SpliceRuntimeContext());
                //set the offset that this scan needs to roll from
                try {
                    currentScan = table.getScanner(next);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
                return getResult();
            }
            try {
                Result next = currentScan.next();
                //should never happen, but it's good to be safe
                if (next == null) return null;

                byte[] value = next.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES, OFFSET_RESULTS_COL);
                if (value == null) {
                    return next;
                } else {
                    //we've exhausted a region without exhausting the offset, so we need
                    //to parse out how many we've skipped and adjust our offset accordingly
                    long skippedInRegion = Bytes.toLong(value);
                    rowsSkipped += skippedInRegion;
                    currentScan.close();
                    currentScan = null;
                    return getResult();
                }
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public Scan toScan() {
            return fullScan;
        }

        @Override
        public void open() {
            table = SpliceAccessManager.getHTable(getTableName());
            try {
                splitScansAroundRegionBarriers();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] getTableName() {
            return tableName;
        }

        @Override
        public void close() {
            try{
                table.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally{
                super.close();
            }
        }

        private static final Function<HRegionLocation,HRegionInfo> infoFunction = new Function<HRegionLocation, HRegionInfo>() {
            @Override
            public HRegionInfo apply(@Nullable HRegionLocation input) {
                return input.getRegionInfo();
            }
        };
        private void splitScansAroundRegionBarriers() throws ExecutionException,IOException{
            //get the region set for this table
            List<HRegionInfo> regionInfos;
            final HTable hTable = SpliceHTableUtil.toHTable(table);
            if(hTable != null) {
                regionInfos = Lists.newArrayList(hTable.getRegionLocations().keySet());
            } else {
                throw new ExecutionException(new UnsupportedOperationException("Unknown Table type, unable to get Region information. Table type is "+table.getClass()));
            }

//            final MetaScanner.MetaScannerVisitor visitor = new MetaScanner.MetaScannerVisitor() {
//                @Override
//                public boolean processRow(Result rowResult) throws IOException {
//                    byte[] bytes = rowResult.getValue(HConstants.CATALOG_FAMILY,HConstants.REGIONINFO_QUALIFIER);
//                    if(bytes==null){
//                        //TODO -sf- log a message here
//                        return true;
//                    }
//                    HRegionInfo info = Writables.getHRegionInfo(bytes);
//                    Integer tableKey = Bytes.mapKey(info.getTableName());
//                    if(tableNameKey.equals(tableKey)&& !(info.isOffline()||info.isSplit())){
//                        regionInfos.add(info);
//                    }
//                    return true;
//                }
//
//                @Override
//                public void close() throws IOException {
//                    //no-op
//                }
//            };
//
//            try {
//                MetaScanner.metaScan(SpliceUtils.config,visitor);
//            } catch (IOException e) {
//                SpliceLogUtils.error(LOG, "Unable to update region cache", e);
//            }

            List<Pair<byte[],byte[]>> ranges = Lists.newArrayListWithCapacity(regionInfos.size());
            byte[] scanStart = fullScan.getStartRow();
            byte[] scanStop = fullScan.getStopRow();

            if(Bytes.compareTo(scanStart,HConstants.EMPTY_START_ROW)==0&&
                    Bytes.compareTo(scanStop,HConstants.EMPTY_END_ROW)==0){
                //we cover everything
                for(HRegionInfo regionInfo:regionInfos){
                    ranges.add(Pair.newPair(regionInfo.getStartKey(),regionInfo.getEndKey()));
                }
            }else{
                for(HRegionInfo regionInfo:regionInfos){
                    Pair<byte[],byte[]> intersect = BytesUtil.intersect(scanStart,scanStop,regionInfo.getStartKey(),regionInfo.getEndKey());
                    if(intersect!=null)
                        ranges.add(intersect);
                }
            }

            //make sure we're sorted low to high
            Collections.sort(ranges,new Comparator<Pair<byte[], byte[]>>() {
                @Override
                public int compare(Pair<byte[], byte[]> o1, Pair<byte[], byte[]> o2) {
                    byte[] left = o1.getFirst();
                    byte[] right = o2.getFirst();
                    if(Bytes.compareTo(left,HConstants.EMPTY_START_ROW)==0){
                        if(Bytes.compareTo(right,HConstants.EMPTY_START_ROW)==0)
                            return 0;
                        else
                            return -1;
                    }else if(Bytes.compareTo(right,HConstants.EMPTY_START_ROW)==0)
                        return 1;
                    else return Bytes.compareTo(left, right);
                }
            });

            offsetScans = new LinkedList<Scan>();
            for(Pair<byte[],byte[]> region:ranges){
                Scan scan = new Scan();
                scan.setStartRow(region.getFirst());
                scan.setStopRow(region.getSecond());
//                scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,
//                        fullScan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS));
                scan.setFilter(fullScan.getFilter());
                if(totalOffset<fullScan.getCaching()){
                    scan.setCaching((int)totalOffset);
                }
                offsetScans.add(scan);
            }
        }

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			// TODO Auto-generated method stub
			return null;
		}
    }

    private class LimitedRowProvider implements RowProvider {
        private final RowProvider provider;
        private final long fetchLimit;
        private long currentRowCount = 0;

        public LimitedRowProvider(RowProvider provider,long fetchLimit) {
            this.provider = provider;
            this.fetchLimit = fetchLimit;
        }

        @Override public void open() throws StandardException { 
        	provider.open(); 
        }
        @Override public void close() { provider.close(); }
        @Override public RowLocation getCurrentRowLocation() { return provider.getCurrentRowLocation(); }
        @Override public byte[] getTableName() { return provider.getTableName(); }
        @Override public int getModifiedRowCount() { return provider.getModifiedRowCount(); }

        @Override
        public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
            return provider.shuffleRows(instructions);
        }

        @Override
        public boolean hasNext() throws StandardException, IOException {
            return currentRowCount<fetchLimit && provider.hasNext();
        }

        @Override
        public ExecRow next() throws StandardException, IOException {
            currentRowCount++;
            return provider.next();
        }

		@Override
		public SpliceRuntimeContext getSpliceRuntimeContext() {
			// TODO Auto-generated method stub
			return null;
		}
    }
}
