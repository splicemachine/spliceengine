package com.splicemachine.derby.impl.sql.execute.operations.batchonce;

import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLRef;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.OperationResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Replaces a ProjectRestrictNode (below an Update) that would otherwise invoke a subquery for each source row.  This
 * operation will read a batch of rows from the source, transform them into the shape expected by update [old value, new
 * value, row location], execute the subquery once, and populate new value column of matching rows with the result.
 *
 * A complication is that we are using this operations where the subquery is expected to return at most one row for each
 * source row and must continue to enforce this, throwing LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION if more than one
 * subquery row is returned for each source row.
 *
 * Example Query: update A set A.name = (select B.name from B where A.id = B.id) where A.name is null;
 *
 * Currently we ONLY support BatchOnce for queries that have a single subquery and where that subquery has a single
 * equality predicate with one correlated column reference.
 *
 * Currently BatchOnce is only used in the case where the subquery tree has a FromBaseTable leaf, no index. So with
 * or without BatchOnce we always scan the entire subquery table.  With BatchOnce however the number of times
 * we scan the entire subquery tables is potentially divided by this class's configurable BATCH_SIZE.  This
 * can make a 24+hour query execute in a few tens of seconds.
 *
 * Related: BatchOnceNode, BatchOnceVisitor, OnceOperation
 */
public class BatchOnceOperation extends SpliceBaseOperation {

    private static final Logger LOG = Logger.getLogger(BatchOnceOperation.class);
    private static final int BATCH_SIZE = SpliceConstants.batchOnceBatchSize;

    /* Constants for the ExecRow this operation emits. */
    private static final int OLD_COL = 1;
    private static final int NEW_COL = 2;
    private static final int ROW_LOC_COL = 3;

    /* Collection of ExecRows we have read from the source and updated with results from the subquery but
     * not yet returned to the operation above us. */
    private final Queue<ExecRow> rowQueue = Lists.newLinkedList();

    private SpliceOperation source;
    private SpliceOperation subquerySource;
    private CursorResultSet cursorResultSet;
    /* Name of the activation field that contains the Update result set. Used to get currentRowLocation */
    private String updateResultSetFieldName;

    /* 1-based column positions for columns in the source we will need.  Will be -1 if not available. */
    private int sourceRowLocationColumnPosition;
    private int sourceCorrelatedColumnPosition;
    private int subqueryCorrelatedColumnPosition;

    public BatchOnceOperation() {
    }

    public BatchOnceOperation(SpliceOperation source,
                              Activation activation,
                              int rsNumber,
                              SpliceOperation subquerySource,
                              String updateResultSetFieldName,
                              int sourceRowLocationColumnPosition,
                              int sourceCorrelatedColumnPosition,
                              int subqueryCorrelatedColumnPosition) throws StandardException {
        super(activation, rsNumber, 0d, 0d);
        this.source = source;
        this.subquerySource = subquerySource.getLeftOperation();
        this.updateResultSetFieldName = updateResultSetFieldName;
        this.sourceRowLocationColumnPosition = sourceRowLocationColumnPosition;
        this.sourceCorrelatedColumnPosition = sourceCorrelatedColumnPosition;
        this.subqueryCorrelatedColumnPosition = subqueryCorrelatedColumnPosition;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Return the next row from our queue.  Fill the queue if necessary.
     */
    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        // return rows from the queue
        if (!rowQueue.isEmpty()) {
            return rowQueue.remove();
        }

        fillQueueIfPossible(spliceRuntimeContext);

        return rowQueue.isEmpty() ? null : rowQueue.remove();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return ImmutableList.of();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return "BatchOnceOperation:" + indent
                + "resultSetNumber:" + resultSetNumber + indent
                + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public String getOptimizerOverrides(){
        return source.getOptimizerOverrides();
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if (source != null) {
            source.open();
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        source.close();
    }

    // - - - - - - - - - - - - - - - - - - -
    // serialization
    // - - - - - - - - - - - - - - - - - - -

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.source = (SpliceOperation) in.readObject();
        this.subquerySource = (SpliceOperation) in.readObject();
        this.updateResultSetFieldName = in.readUTF();
        this.sourceRowLocationColumnPosition = in.readInt();
        this.sourceCorrelatedColumnPosition = in.readInt();
        this.subqueryCorrelatedColumnPosition = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.source);
        out.writeObject(this.subquerySource);
        out.writeUTF(this.updateResultSetFieldName);
        out.writeInt(this.sourceRowLocationColumnPosition);
        out.writeInt(this.sourceCorrelatedColumnPosition);
        out.writeInt(this.subqueryCorrelatedColumnPosition);
    }

    // - - - - - - - - - - - - - - - - - - -
    // private / non-interface methods
    // - - - - - - - - - - - - - - - - - - -

    /*
     * Read up to BATCH_SIZE rows from the source, transform them to the format expected by the update operation
     * above us [old value, new value, rowLocation], and populate the new value with results from subquery.
     *
     * Possible future optimization: fill queue in background.
     */
    private void fillQueueIfPossible(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        //
        // STEP 1: Read BATCH_SIZE rows from the source
        //
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // for quickly finding source rows with a given key
        Multimap<DataValueDescriptor, ExecRow> sourceRowsMap = ArrayListMultimap.create(BATCH_SIZE, 1);
        DataValueDescriptor nullValue = this.source.getExecRowDefinition().cloneColumn(1).getNewNull();

        ExecRow sourceRow;
        ExecRow newRow;
        while ((sourceRow = source.nextRow(spliceRuntimeContext)) != null && rowQueue.size() < BATCH_SIZE) {
            sourceRow = sourceRow.getClone();
            DataValueDescriptor sourceKey = sourceRow.getColumn(sourceCorrelatedColumnPosition);
            DataValueDescriptor sourceOldValue = sourceRow.getColumn(sourceCorrelatedColumnPosition == 1 ? 2 : 1);

            newRow = new ValueRow(3);
            //
            // old value from source
            //
            newRow.setColumn(OLD_COL, sourceOldValue);
            //
            // new value will (possibly) come from subquery (subquery could return null, or return no row)
            //
            newRow.setColumn(NEW_COL, nullValue);
            //
            // row location
            //
            HBaseRowLocation currentRowLocation;
            if(sourceRowLocationColumnPosition >= 1) {
                /* When the source operation is scanning the target table we can get the current row location
                 * from our activation.  When the source operation is sinking (a sinking join for example) we
                 * instead get the current row location from the source row. */
                SQLRef roLocCol = (SQLRef) sourceRow.getColumn(sourceRowLocationColumnPosition);
                currentRowLocation = (HBaseRowLocation) roLocCol.getObject();
            }
            else {
                HBaseRowLocation rowLocationFromActivation = getRowLocationFromActivation();
                currentRowLocation = HBaseRowLocation.deepClone(rowLocationFromActivation);
            }
            newRow.setColumn(ROW_LOC_COL, new SQLRef(currentRowLocation));

            sourceRowsMap.put(sourceKey, newRow);
            rowQueue.add(newRow);
        }

        /* Don't execute the subquery again if there were no more source rows. */
        if (rowQueue.isEmpty()) {
            return;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        //
        // STEP 2: Populate source row columns with values from subquery.  This will read the entire subquery
        // table every time.  Even if all rows for the batch are found quickly at the beginning of the subquery's scan
        // we must scan the entire table to throw LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION if appropriate.
        //
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        OperationResultSet operationResultSet = new OperationResultSet(activation, subquerySource);
        try {
            operationResultSet.openCore();
            ExecRow nextRowCore;
            Set<DataValueDescriptor> uniqueKeySet = Sets.newHashSetWithExpectedSize(BATCH_SIZE);
            while ((nextRowCore = operationResultSet.getNextRowCore()) != null) {
                nextRowCore = nextRowCore.getClone();
                DataValueDescriptor keyColumn = nextRowCore.getColumn(subqueryCorrelatedColumnPosition);
                Collection<ExecRow> correspondingSourceRows = sourceRowsMap.get(keyColumn);
                for (ExecRow correspondingSourceRow : correspondingSourceRows) {
                    correspondingSourceRow.setColumn(NEW_COL, nextRowCore.getColumn(subqueryCorrelatedColumnPosition == 1 ? 2 : 1));
                }
                if (!uniqueKeySet.add(keyColumn)) {
                    throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                }
            }
        } finally {
            operationResultSet.close();
        }
    }


    private HBaseRowLocation getRowLocationFromActivation() throws StandardException {
        BaseActivation activation = (BaseActivation) this.getActivation();
        try {
            return (HBaseRowLocation) getCursorResultSet(activation).getRowLocation();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private CursorResultSet getCursorResultSet(BaseActivation activation) throws NoSuchFieldException, IllegalAccessException {
        if (cursorResultSet == null) {
            Field field = activation.getClass().getDeclaredField(updateResultSetFieldName);
            field.setAccessible(true);
            cursorResultSet = (CursorResultSet) field.get(activation);
        }
        return cursorResultSet;
    }
}
