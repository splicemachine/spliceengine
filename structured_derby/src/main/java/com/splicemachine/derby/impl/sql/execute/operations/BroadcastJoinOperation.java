package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.splicemachine.constants.bytes.HashableBytes;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.shared.common.reference.MessageId;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class BroadcastJoinOperation extends JoinOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(BroadcastJoinOperation.class);
    protected String emptyRowFunMethodName;
    protected boolean wasRightOuterJoin;
    protected Qualifier[][] qualifierProbe;
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected ExecRow rightTemplate;
    protected static List<NodeType> nodeTypes;
    protected Scan reduceScan;
    protected RowProvider clientProvider;
    protected SQLInteger rowType;
    protected byte[] priorHash;
    protected List<ExecRow> rights;
    protected byte[] rightHash;
    protected Hasher leftHasher;
    protected Iterator<ExecRow> rightIterator;
    protected BroadcastNextRowIterator broadcastIterator;
    protected Map<HashableBytes, List<ExecRow>> rightSideMap;
    protected Map<byte[], List<ExecRow>> rightSideList;
    protected static final Cache<String, Map<HashableBytes, List<ExecRow>>> broadcastJoinCache;


    static {
        nodeTypes = new ArrayList<NodeType>();
        nodeTypes.add(NodeType.MAP);
        nodeTypes.add(NodeType.SCROLL);
        broadcastJoinCache = CacheBuilder.newBuilder().
                maximumSize(50000).expireAfterWrite(10, TimeUnit.MINUTES).removalListener(new RemovalListener<String, Map<HashableBytes, List<ExecRow>>>() {
            @Override
            public void onRemoval(RemovalNotification<String, Map<HashableBytes, List<ExecRow>>> notification) {
                SpliceLogUtils.trace(LOG, "Removing unique sequence ID %s", notification.getKey());
            }
        }).build();
    }

    public BroadcastJoinOperation() {
        super();
    }

    public BroadcastJoinOperation(NoPutResultSet leftResultSet,
                                  int leftNumCols,
                                  NoPutResultSet rightResultSet,
                                  int rightNumCols,
                                  int leftHashKeyItem,
                                  int rightHashKeyItem,
                                  Activation activation,
                                  GeneratedMethod restriction,
                                  int resultSetNumber,
                                  boolean oneRowRightSide,
                                  boolean notExistsRightSide,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides) throws StandardException {
        super(leftResultSet, leftNumCols, rightResultSet, rightNumCols,
                activation, restriction, resultSetNumber, oneRowRightSide, notExistsRightSide,
                optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG, "instantiate");
        this.leftHashKeyItem = leftHashKeyItem;
        this.rightHashKeyItem = rightHashKeyItem;
        init(SpliceOperationContext.newContext(activation));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SpliceLogUtils.trace(LOG, "readExternal");
        super.readExternal(in);
        leftHashKeyItem = in.readInt();
        rightHashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        SpliceLogUtils.trace(LOG, "getNextRowCore");
        if (rightSideMap == null)
            rightSideMap = retrieveRightSideCache();

        if (broadcastIterator == null || !broadcastIterator.hasNext()) {
            if ((leftRow = leftResultSet.getNextRowCore()) == null) {
                mergedRow = null;
                this.setCurrentRow(mergedRow);
                return mergedRow;
            } else {
                broadcastIterator = new BroadcastNextRowIterator(false, leftRow, leftHasher);
                return getNextRowCore();
            }
        } else {
            return broadcastIterator.next();
        }
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return leftResultSet.getReduceRowProvider(top, template);
        /*
        if(clientProvider==null){
            SpliceUtils.setInstructions(reduceScan,activation,top);
            clientProvider = new ClientScanProvider(SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,template,null);
        }
        return clientProvider;
        */
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return leftResultSet.getMapRowProvider(top, template);
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        leftHashKeys = generateHashKeys(leftHashKeyItem, (SpliceBaseOperation) this.leftResultSet);
        rightHashKeys = generateHashKeys(rightHashKeyItem, (SpliceBaseOperation) this.rightResultSet);
        mergedRow = activation.getExecutionFactory().getValueRow(leftNumCols + rightNumCols);
        rightTemplate = activation.getExecutionFactory().getValueRow(rightNumCols);
        leftHasher = new Hasher(leftRow.getRowArray(), leftHashKeys, null);
        rightResultSet.init(context);
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        final List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
        this.generateLeftOperationStack(opStack);
        SpliceLogUtils.trace(LOG, "operationStack=%s", opStack);

        // Get the topmost value, instead of the bottommost, in case it's you
        SpliceOperation regionOperation = opStack.get(opStack.size() - 1);
        SpliceLogUtils.trace(LOG, "regionOperation=%s", opStack);
        RowProvider provider;
        if (regionOperation.getNodeTypes().contains(NodeType.REDUCE)) {
            provider = regionOperation.getReduceRowProvider(this, getExecRowDefinition());
        } else {
            provider = regionOperation.getMapRowProvider(this, getExecRowDefinition());
        }
        return new SpliceNoPutResultSet(activation, this, provider);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        JoinUtils.getMergedRow(((SpliceOperation) this.leftResultSet).getExecRowDefinition(), ((SpliceOperation) this.rightResultSet).getExecRowDefinition(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
        return mergedRow;
    }

    @Override
    public List<NodeType> getNodeTypes() {
        SpliceLogUtils.trace(LOG, "getNodeTypes");
        return nodeTypes;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        SpliceLogUtils.trace(LOG, "getLeftOperation");
        return leftResultSet;
    }

    protected class BroadcastNextRowIterator implements Iterator<ExecRow> {
        protected Iterator<ExecRow> rightSideIterator;
        protected ExecRow leftRow;
        protected ExecRow rightRow;
        protected boolean outerJoin;
        protected Hasher leftHasher;
        protected boolean seenRow;

        public BroadcastNextRowIterator(boolean outerJoin, ExecRow leftRow, Hasher leftHasher) throws StandardException {
            this.leftRow = leftRow;
            this.outerJoin = outerJoin;
            this.leftHasher = leftHasher;
            List<ExecRow> rows = rightSideMap.get(new HashableBytes(leftHasher.generateSortedHashKeyWithoutUniqueKey(leftRow.getRowArray())));
            if (rows != null) {
            	rightSideIterator = rows.iterator();
            }
        }

        @Override
        public boolean hasNext() {
            if (rightSideIterator != null && rightSideIterator.hasNext()) {
                mergedRow = JoinUtils.getMergedRow(leftRow, rightSideIterator.next(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
                setCurrentRow(mergedRow);
                currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
                SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
                return true;
            }
            if (rightSideIterator == null && outerJoin) {
                mergedRow = JoinUtils.getMergedRow(leftRow, getEmptyRow(), wasRightOuterJoin, rightNumCols, leftNumCols, mergedRow);
                setCurrentRow(mergedRow);
                currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
                SpliceLogUtils.trace(LOG, "current row returned %s", currentRow);
                return true;
            }

            return false;
        }

        @Override
        public ExecRow next() {
            SpliceLogUtils.trace(LOG, "next row=" + mergedRow);
            return mergedRow;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Cannot Be Removed - Not Implemented!");
        }
    }

    protected ExecRow getEmptyRow() {
        throw new RuntimeException("Should only be called on outer joins");
    }

    private Map<HashableBytes, List<ExecRow>> retrieveRightSideCache() throws StandardException {
        try {
            // Cache population is what we want here concurrency-wise: only one Callable will be invoked to
            // populate the cache for a given key; any other concurrent .get(k, callable) calls will block
            return broadcastJoinCache.get(uniqueSequenceID, new Callable<Map<HashableBytes, List<ExecRow>>>() {
                @Override
                public Map<HashableBytes, List<ExecRow>> call() throws Exception {
                    SpliceLogUtils.trace(LOG, "Load right-side cache for BroadcastJoin, uniqueSequenceID " + uniqueSequenceID);
                    return loadRightSide();
                }
            });
        } catch (Exception e) {
            throw StandardException.newException(MessageId.SPLICE_GENERIC_EXCEPTION, e,
                    "Problem loading right-hand cache for BroadcastJoin, uniqueSequenceID " + uniqueSequenceID);
        }
    }

    private Map<HashableBytes, List<ExecRow>> loadRightSide() throws StandardException, IOException {
        HashableBytes hashKey;
        List<ExecRow> rows;
        Map<HashableBytes, List<ExecRow>> cache = new HashMap<HashableBytes, List<ExecRow>>();
        Hasher hasher = new Hasher(rightRow.getRowArray(), rightHashKeys, null);
        NoPutResultSet resultSet = rightResultSet.executeScan();
        resultSet.openCore();

        while ((rightRow = resultSet.getNextRowCore()) != null) {
            hashKey = new HashableBytes(hasher.generateSortedHashKeyWithoutUniqueKey(rightRow.getRowArray()));
            if ((rows = cache.get(hashKey)) != null) {
                rows.add(rightRow);
            } else {
                rows = new ArrayList<ExecRow>();
                rows.add(rightRow);
                cache.put(hashKey, rows);
            }
        }
        return Collections.unmodifiableMap(cache);
    }

}