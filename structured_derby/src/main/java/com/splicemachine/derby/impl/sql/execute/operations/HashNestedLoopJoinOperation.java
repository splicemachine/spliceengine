package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.splicemachine.collections.RingBuffer;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.storage.AndPredicate;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.OrPredicate;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 *
 * Batch NLJ is only applicable for a TableScan on the right hand side. Anything else won't work.
 *
 * It will also be pretty inefficient for anything except oneRowRightSide type joins.
 *
 * @author Scott Fines
 * Date: 7/22/14
 */
public class HashNestedLoopJoinOperation extends JoinOperation{
//    private static final Logger LOG = Logger.getLogger(HashNestedLoopJoinOperation.class);
    private static final List<NodeType> nodeTypes = ImmutableList.of(NodeType.MAP, NodeType.SCROLL);

    private int leftHashKeyItem;
    private int rightHashKeyItem;

    private int[] leftHashKeys;
    private int[] rightHashKeys;

    private RingBuffer<ExecRow> leftRowBuffer;
    private DualHashHashTable<ExecRow> rightHashTable;
    private boolean returnedRight;

    private ScanProvider scanProvider;

    public HashNestedLoopJoinOperation(){}

    public HashNestedLoopJoinOperation(SpliceOperation leftResultSet, int leftNumCols,
                                       SpliceOperation rightResultSet, int rightNumCols,
                                       int leftHashKeyItem, int rightHashKeyItem,
                                       Activation activation, GeneratedMethod restriction,
                                       int resultSetNumber, boolean oneRowRightSide,
                                       GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
                                       boolean notExistsRightSide,
                                       double optimizerEstimatedRowCount, double optimizerEstimatedCost,
                                       String userSuppliedOptimizerOverrides) throws StandardException{
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide,
                notExistsRightSide, optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        this.emptyRowFunMethodName = (emptyRowFun == null) ? null : emptyRowFun.getMethodName();
        this.wasRightOuterJoin = wasRightOuterJoin;
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override public List<NodeType> getNodeTypes() { return nodeTypes; }
    @Override public String toString() { return "BatchNestedLoop" + super.toString(); }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        rightHashKeyItem = in.readInt();
        leftHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(rightHashKeyItem);
        out.writeInt(leftHashKeyItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem);
        rightHashKeys = generateHashKeys(rightHashKeyItem);
        startExecutionTime = System.currentTimeMillis();

        SpliceOperation rightOp = rightResultSet;
        while(rightOp!=null){
            if(rightOp instanceof ScanOperation){
                assert !(rightOp instanceof MultiProbeTableScanOperation): "HashNestedLoopJoin does not currently support multi-probe table scans";
                scanProvider = new BaseScanProvider((ScanOperation)rightOp);
                break;
            }else
                rightOp = rightOp.getLeftOperation();
        }
        assert scanProvider!=null: "Cannot currently use HashNestedLoopJoin on a non-table-scan right side";
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(timer == null) {
            timer = spliceRuntimeContext.newTimer();
            timer.startTiming();
        }
        return next(false,spliceRuntimeContext);
    }

    protected ExecRow next(boolean outerJoin,SpliceRuntimeContext context) throws StandardException,IOException{
        /*
         * The algorithm is as follows:
         *
         * 1. Fill a read buffer with rows from the left-hand side
         * 2. For each row in the buffer, get the qualifiers and start/stop keys as if we were a normal NestedLoopJoin
         * 3. Construct a single Qualifier list for non-key qualifiers(essentially an or clause around
         * all the possible clauses)
         * 4. Construct a skip scan filter to seek to the proper row key ranges
         * 5. Issue a single scan to pull back the proper rows.
         */
        if(leftRowBuffer==null || leftRowBuffer.size()<=0){
            nextBatch(context);
        }

        ExecRow leftN = leftRowBuffer.peek();
        ExecRow nextLeft = leftN;
        if(leftN==null) {
            clearCurrentRow();
            timer.stopTiming();
            return null;
        }
        RingBuffer<ExecRow> rightExecRows = null;

        while(leftN!=null){
            //look in the right hash table to find all the ExecRows which match the query
            if(rightExecRows==null)
                rightExecRows = rightHashTable.get(leftN);

            ExecRow right = null;
            if(rightExecRows!=null){
                right = rightExecRows.next();
                if(rightExecRows.size()<=0){
                /*
                 * The right side for this left side is exhausted, so we
                 * need to proceed to the next left row. If the next
                 * left row batch is exhausted, we need to fetch a new batch
                 * of records
                 */
                    rightExecRows.readReset();
                    nextLeft = advanceLeft(context);
                    rightExecRows=null;
                }
            }else if(allowEmptyRow(leftN)){
               nextLeft = advanceLeft(context); //seek past to avoid duplication
            }

            if(notExistsRightSide){
                right = (right==null)? getEmptyRow() : null;
            }

            if(outerJoin && right==null){
                right = getEmptyRightRow();
            }

            if(right==null){
                //there are no more right rows to see
                rowsSeenLeft++;
                nextLeft = advanceLeft(context);
                rightExecRows = null;
                leftN = nextLeft;
                continue;
            }

            if(oneRowRightSide && returnedRight){
                returnedRight=false;
                //skip this row because it's already been found
                continue;
            }

            rowsSeenRight++;
            nonNullRight();
            returnedRight=true;

            mergedRow = JoinUtils.getMergedRow(leftN,right,false,rightNumCols,leftNumCols,mergedRow);
            rightResultSet.setCurrentRow(right);
            leftResultSet.setCurrentRow(leftN);
            setCurrentRow(mergedRow);

            if(restriction!=null){
                DataValueDescriptor restrictBoolean = restriction.invoke();
                //TODO -sf- is this truly correct? It doesn't seem that way
                if(restrictBoolean.isNull() || restrictBoolean.getBoolean()){
                    timer.tick(1);
                    return mergedRow;
                }
            }

            leftN = nextLeft;
        }
        clearCurrentRow();

        timer.stopTiming();
        return null;
    }

    protected boolean allowEmptyRow(ExecRow leftN) {
        return false;
    }

    protected ExecRow advanceLeft(SpliceRuntimeContext context) throws IOException, StandardException {
        leftRowBuffer.readAdvance(); //clear the left row
        this.returnedRight = false;
        if(leftRowBuffer.size()<=0){
            nextBatch(context);
        }
        return leftRowBuffer.peek();
    }

    protected void nonNullRight() {
        //no-op for inner loops
    }

    protected ExecRow getEmptyRightRow() throws StandardException {
        return null; //for inner joins, return null here
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        stats.addMetric(OperationMetric.INPUT_ROWS, rowsSeenLeft);
        stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, rowsSeenRight);
        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, bytesReadRight);
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteScanWallTime);
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteScanCpuTime);
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteScanUserTime);
    }

    /*****************************************************************************************************************/
    /*private helper methods*/

    private static final Function<ScanRange, byte[]> toPredicateFunction = new Function<ScanRange, byte[]>() {
        @SuppressWarnings("ConstantConditions")
        @Nullable @Override
        public byte[] apply(@Nullable ScanRange input) {
            return input.predicate;
        }
    };

    private static final Function<ScanRange, Pair<byte[],byte[]>> toPairFunction = new Function<ScanRange, Pair<byte[],byte[]>>() {
        @SuppressWarnings("ConstantConditions")
        @Nullable @Override
        public Pair<byte[],byte[]> apply(@Nullable ScanRange input) {
            return Pair.newPair(input.start,input.stop);
        }
    };

    private void nextBatch(SpliceRuntimeContext context) throws IOException, StandardException {
        if(leftRowBuffer==null)
            leftRowBuffer = new RingBuffer<ExecRow>(SpliceConstants.hashNLJLeftRowBufferSize); 
        if(rightHashTable==null){
            DualHashHashTable.EntryHasher<ExecRow> rightEntryHasher = new DualHashHashTable.EntryHasher<ExecRow>() {
                @Override
                public int hash(ExecRow row) {
                    int pos = 17;
                    DataValueDescriptor[] dvds = row.getRowArray();
                    for(int key:rightHashKeys){
                        pos+= 31*pos+dvds[key].hashCode();
                    }
                    return pos;
                }

                @Override
                public boolean equalsOnHash(ExecRow left, ExecRow right) {
                    DataValueDescriptor[] leftDvds = left.getRowArray();
                    DataValueDescriptor[] rightDvds = right.getRowArray();
                    for(int key:rightHashKeys){
                        if(!leftDvds[key].equals(rightDvds[key]))
                            return false;
                    }
                    return true;
                }
            };

            DualHashHashTable.EntryHasher<ExecRow> leftEntryHasher = new DualHashHashTable.EntryHasher<ExecRow>(){
                @Override
                public int hash(ExecRow row) {
                    int pos = 17;
                    DataValueDescriptor[] dvds = row.getRowArray();
                    for(int key:leftHashKeys){
                        pos+= 31*pos+dvds[key].hashCode();
                    }
                    return pos;
                }

                @Override
                public boolean equalsOnHash(ExecRow left, ExecRow right) {
                    DataValueDescriptor[] leftDvds = left.getRowArray();
                    DataValueDescriptor[] rightDvds = right.getRowArray();
                    for(int i=0;i<leftHashKeys.length;i++){
                        DataValueDescriptor leftDvd = leftDvds[leftHashKeys[i]];
                        DataValueDescriptor rightDvd = rightDvds[rightHashKeys[i]];
                        if(!leftDvd.equals(rightDvd)) return false;
                    }
                    return true;
                }
            };
            
            rightHashTable = new DualHashHashTable<ExecRow>(SpliceConstants.hashNLJRightHashTableSize, rightEntryHasher, leftEntryHasher);
        } else {
            rightHashTable.clear();
        }
        //fill the left side buffer
        for(int i=0;i<leftRowBuffer.bufferSize();i++){
            ExecRow element = leftResultSet.nextRow(context);
            if(element==null)
                break;
            leftRowBuffer.add(element.getClone());
        }

        /*
         * for each left-hand row, construct an OperationResultSet and add the scan
         */
        int remaining = leftRowBuffer.size();
        if(remaining<=0) return; //no more data to get
        leftRowBuffer.mark();

        List<ScanRange> keyData = Lists.newArrayListWithCapacity(remaining);
        @SuppressWarnings("SuspiciousNameCombination") OperationResultSet rightRs = new OperationResultSet(activation,rightResultSet);
        while(remaining>0){
            ExecRow next = leftRowBuffer.next();
            leftResultSet.setCurrentRow(next);
            setCurrentRow(next);
            rightRs.sinkOpen(true,false);
            Scan nonSIScan = scanProvider.get(context);
            byte[] startRow = nonSIScan.getStartRow();

            addRangeAndPredicate(0,startRow, nonSIScan.getStopRow(), nonSIScan.getAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL), keyData);
            remaining--;
        }
        leftRowBuffer.readReset();

        /*
         * create the skip-scan filter
         */
        List<byte[]> predicates = Lists.transform(keyData, toPredicateFunction);
        List<Pair<byte[], byte[]>> ranges = Lists.transform(keyData, toPairFunction);
        SkippingScanFilter filter = new SkippingScanFilter(ranges,predicates);
        Scan scan = new Scan(ranges.get(0).getFirst(),ranges.get(ranges.size()-1).getSecond());
        scan.setFilter(filter);
        scan.setMaxVersions();
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scanProvider.set(scan,context);


        //execute the scan
        rightRs.executeScan(false,context);

        SpliceNoPutResultSet nprs = rightRs.getDelegate();
        try{
            ExecRow rightRow = nprs.getNextRow();
            while(rightRow!=null){
                rightHashTable.add(rightRow.getClone()); //TODO -sf- can we do better than cloning every time?
                rightRow = nprs.getNextRow();
            }
            rightHashTable.markAllBuffers();
            if(shouldRecordStats()) {
                IOStats ioStats = rightRs.getStats();
                bytesReadRight += ioStats.getBytes();
                remoteScanCpuTime += ioStats.getTime().getCpuTime();
                remoteScanUserTime += ioStats.getTime().getUserTime();
                remoteScanWallTime += ioStats.getTime().getWallClockTime();
            }
        }finally{
            rightRs.close();
        }
    }

    private void addRangeAndPredicate(int position, byte[] startKey,byte[] stopKey,byte[] predicateBytes, List<ScanRange> startStopKeys) throws IOException {
        if(position>=startStopKeys.size()){
            startStopKeys.add(new ScanRange(startKey,stopKey,predicateBytes));
            return;
        }
        ScanRange range = startStopKeys.get(position);
        byte[] s = range.start;
        byte[] e = range.stop;
        int startEndCompare = BytesUtil.startComparator.compare(startKey, e);
        if(startEndCompare >0 || (startEndCompare==0 && e.length>0)){
            //this element is entirely before us, so move to the next entry
            addRangeAndPredicate(position + 1, startKey, stopKey, predicateBytes, startStopKeys);
            return;
        }
        int endStartCompare = BytesUtil.endComparator.compare(stopKey,s);
        if(endStartCompare<0||endStartCompare==0 && s.length>0){
            //this range is entirely after us, so add us in before and return
            startStopKeys.add(position, new ScanRange(startKey,stopKey,predicateBytes));
            return;
        }

        /*
         * We know that there MUST be overlap on the two ranges, just need to find out which.
         * Then we change this range to the small element, and recursively add the range on after this
         */
        int startCompare = BytesUtil.startComparator.compare(startKey,s);
        if(startCompare<0){
            //start < s --> [start,s,predicateBytes) + extra
            startStopKeys.add(position,new ScanRange(startKey,s,predicateBytes));
            int endCompare = BytesUtil.endComparator.compare(stopKey,e);
            if(endCompare<0){
                //stop < e --> [s,stop,mergedPredicate) U recurse(stop,e,oldPredicate)
                byte[] oldPred = range.predicate;
                range.stop = stopKey;
                range.predicate = mergePredicates(predicateBytes,oldPred);
                addRangeAndPredicate(position+2,stopKey,e,oldPred,startStopKeys);
            }else if(endCompare==0){
                //stop ==e
                range.predicate = mergePredicates(predicateBytes,range.predicate);
            }else{
                //stop > e
                range.predicate = mergePredicates(predicateBytes,range.predicate);
                addRangeAndPredicate(position+2,e,stopKey,predicateBytes,startStopKeys);
            }
        }else if(startCompare==0){
            //start == s --> [s,min) + extra
            int endCompare = BytesUtil.endComparator.compare(stopKey,e);
            if(endCompare<0){
                //stop < e
                byte[] oldPred = range.predicate;
                range.stop = stopKey;
                range.predicate = mergePredicates(predicateBytes,oldPred);
                addRangeAndPredicate(position+1,stopKey,e,oldPred,startStopKeys);
            }else if(endCompare==0){
                //stop ==e
                range.predicate = mergePredicates(predicateBytes,range.predicate);
            }else{
                //stop > e
                range.predicate = mergePredicates(predicateBytes,range.predicate);
                addRangeAndPredicate(position+1,e,stopKey,predicateBytes,startStopKeys);
            }
        }else{
            //start > s --> [s,start,oldPred)+ extra
            range.stop = startKey;
            int endCompare = BytesUtil.endComparator.compare(stopKey,e);
            if(endCompare<0){
                //stop < e --> [start,stop), merged + extra
                ScanRange newScan = new ScanRange(startKey, stopKey, mergePredicates(predicateBytes, range.predicate));
                if(position>=startStopKeys.size()-1)
                    startStopKeys.add(newScan);
                else
                    startStopKeys.add(position+1,newScan);
                addRangeAndPredicate(position+2,stopKey,e,range.predicate,startStopKeys);
            }else if(endCompare==0){
                //stop ==e
                ScanRange newScan = new ScanRange(startKey, stopKey, mergePredicates(predicateBytes, range.predicate));
                if(position>=startStopKeys.size()-1)
                    startStopKeys.add(newScan);
                else
                    startStopKeys.add(position+1,newScan);
            }else{
                //stop > e
                ScanRange newScan = new ScanRange(startKey, e, mergePredicates(predicateBytes, range.predicate));
                if(position>=startStopKeys.size()-1)
                    startStopKeys.add(newScan);
                else
                    startStopKeys.add(position+1, newScan);
                addRangeAndPredicate(position+2,e,stopKey,predicateBytes,startStopKeys);
            }
        }
    }

    private byte[] mergePredicates(byte[] predicateBytes, byte[] otherBytes) throws IOException {
        //in the case where we have the same predicate in both cases, save some effort
        if(Bytes.equals(predicateBytes,otherBytes)) return predicateBytes;
        EntryPredicateFilter filter1 = EntryPredicateFilter.fromBytes(predicateBytes);
        EntryPredicateFilter filter2 = EntryPredicateFilter.fromBytes(otherBytes);
        ObjectArrayList<Predicate> predicates1 = filter1.getValuePredicates();
        ObjectArrayList<Predicate> predicates2 = filter2.getValuePredicates();
        int s2 = predicates2.size();
        Object[] preds2 = predicates2.buffer;
        ObjectArrayList<Predicate> missingPredicates = ObjectArrayList.newInstance();
        for(ObjectCursor<Predicate> pred:predicates1){
            boolean found = false;
            for(int i=0;i<s2;i++){
                Predicate p2 = (Predicate)preds2[i];
                if(pred.value.equals(p2)){
                    found = true;
                    break;
                }
            }
            if(!found){
                missingPredicates.add(pred.value);
            }
        }

        if(missingPredicates.size()>0){
            Predicate andPred1 = AndPredicate.newAndPredicate(predicates2);
            Predicate andPred2 = AndPredicate.newAndPredicate(missingPredicates);
            Predicate orPreds = OrPredicate.or(andPred1, andPred2);

            return new EntryPredicateFilter(filter1.getCheckedColumns(),ObjectArrayList.from(orPreds)).toBytes();
        }else{
            //if all predicates match, we don't need to create a new entity
            return otherBytes;
        }
    }

    private static class ScanRange{
        private byte[] start;
        private byte[] stop;
        private byte[] predicate;

        public ScanRange(byte[] start, byte[] stop, byte[] predicate){
            this.start = start;
            this.stop = stop;
            this.predicate = predicate;
        }
    }

    private static interface ScanProvider{
        Scan get(SpliceRuntimeContext ctx) throws StandardException,IOException;

        void set(Scan newScan,SpliceRuntimeContext ctx) throws StandardException,IOException;
    }

    private static final ScanProvider NOOP_SCAN_PROVIDER = new ScanProvider() {
        @Override public Scan get(SpliceRuntimeContext ctx) throws StandardException, IOException { return null; }
        @Override public void set(Scan newScan, SpliceRuntimeContext ctx) throws StandardException, IOException {  }
    };

    private class BaseScanProvider implements ScanProvider {
        private ScanOperation scanOp;
        public BaseScanProvider(ScanOperation scanOp) {
            this.scanOp = scanOp;
        }

        @Override
        public Scan get(SpliceRuntimeContext ctx) throws StandardException, IOException {
            return scanOp.getNonSIScan(ctx);
        }

        @Override
        public void set(Scan newScan, SpliceRuntimeContext ctx) throws StandardException, IOException {
            scanOp.setScan(newScan);
        }
    }
}
