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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataType;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OnceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.batchonce.BatchOnceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.primitives.Bytes;
import org.spark_project.guava.collect.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 *
 * Created by jyuan on 10/7/15.
 */
public class BatchOnceFunction<Op extends SpliceOperation>
        extends SpliceFlatMapFunction<Op,Iterator<ExecRow>, ExecRow> implements Serializable {

    private boolean initialized;
    private BatchOnceOperation op;
    private SpliceOperation subquerySource;
    private int[] sourceCorrelatedColumnPositions;
    private int[] subqueryCorrelatedColumnPositions;
    private ExecRow sourceExecRow;
    private ExecRow subqueryExecRow;
    private KeyEncoder sourceKeyEncoder;
    private KeyEncoder subqueryKeyEncoder;
    private int sourceColumnPosition;
    private int subqueryColumnPosition;
    private int sourceRowLoationColumnPosition;
    private int cardinalityCheck;

    /* Collection of ExecRows we have read from the source and updated with results from the subquery but
     * not yet returned to the operation above us. */
    private Queue<ExecRow> rowQueue;

    private final int batchSize;

    /* Constants for the ExecRow this operation emits. */
    private static final int OLD_COL = 1;
    private static final int NEW_COL = 2;
    private static final int ROW_LOC_COL = 3;

    public BatchOnceFunction() {
        batchSize = EngineDriver.driver().getConfiguration().getBatchOnceBatchSize();
    }

    public BatchOnceFunction (OperationContext<Op> operationContext) {
        super(operationContext);
        batchSize = EngineDriver.driver().getConfiguration().getBatchOnceBatchSize();
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {

        if (!initialized) {
            init();
            initialized = true;
        }
        //pull a batch of rows
        if (rowQueue.isEmpty()) {
            loadNextBatch(locatedRows);
        }
        return rowQueue.iterator();
    }

    private void init() throws StandardException {
        this.op = (BatchOnceOperation)getOperation();
        this.subquerySource = op.getSubquerySource();
        this.sourceCorrelatedColumnPositions = op.getSourceCorrelatedColumnPositions();
        this.subqueryCorrelatedColumnPositions = op.getSubqueryCorrelatedColumnPositions();
        this.rowQueue = Lists.newLinkedList();
        this.sourceExecRow = op.getSource().getExecRowDefinition();
        this.subqueryExecRow = subquerySource.getExecRowDefinition();
        this.sourceKeyEncoder = getKeyEncoder(sourceCorrelatedColumnPositions, sourceExecRow);
        this.subqueryKeyEncoder = getKeyEncoder(subqueryCorrelatedColumnPositions, subqueryExecRow);
        this.sourceColumnPosition = getColumnPosition(sourceExecRow, sourceCorrelatedColumnPositions);
        this.subqueryColumnPosition = getColumnPosition(subqueryExecRow, subqueryCorrelatedColumnPositions);
        this.sourceRowLoationColumnPosition = op.getSourceRowLocationColumnPosition();
        this.cardinalityCheck = op.getCardinalityCheck();
    }

    private void loadNextBatch(Iterator<ExecRow> locatedRows) throws StandardException, IOException {
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        //
        // STEP 1: Read batchSize rows from the source
        //
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        // for quickly finding source rows with a given key
        Multimap<String, ExecRow> sourceRowsMap = ArrayListMultimap.create(batchSize, 1);
        DataValueDescriptor nullValue = op.getSource().getExecRowDefinition().cloneColumn(1).getNewNull();

        ExecRow sourceRow;
        ExecRow newRow;
        while (locatedRows.hasNext() && rowQueue.size() <batchSize) {
            sourceRow = locatedRows.next();
            byte[] sourceKey = sourceKeyEncoder.getKey(sourceRow);

            newRow = new ValueRow(3);
            DataValueDescriptor sourceOldValue = sourceRow.getColumn(sourceColumnPosition);
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
            newRow.setColumn(ROW_LOC_COL, sourceRow.getColumn(sourceRowLoationColumnPosition));

            sourceRowsMap.put(Bytes.toHex(sourceKey), newRow.getClone());
        }

        /* Don't execute the subquery again if there were no more source rows. */
        if (sourceRowsMap.isEmpty()) {
            return;
        }

        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        //
        // STEP 2: Populate source row columns with values from subquery.  This will read the entire subquery
        // table every time.  Even if all rows for the batch are found quickly at the beginning of the subquery's scan
        // we must scan the entire table to throw LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION if appropriate.
        //
        // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        try {
            subquerySource.openCore();
            Iterator<ExecRow> subqueryIterator = subquerySource.getExecRowIterator();
            ExecRow nextRowCore;
            Map<String, DataValueDescriptor> uniqueKeyMap = Maps.newHashMapWithExpectedSize(batchSize);
            while (subqueryIterator.hasNext()) {
                nextRowCore = subqueryIterator.next().getClone();
                byte[] keyColumn = subqueryKeyEncoder.getKey(nextRowCore);
                String keyAsString = Bytes.toHex(keyColumn);
                Collection<ExecRow> correspondingSourceRows = sourceRowsMap.get(keyAsString);
                for (ExecRow correspondingSourceRow : correspondingSourceRows) {
                    correspondingSourceRow.setColumn(NEW_COL, nextRowCore.getColumn(subqueryColumnPosition));
                    rowQueue.add(correspondingSourceRow);
                }
                switch (this.cardinalityCheck) {
                    case OnceOperation.NO_CARDINALITY_CHECK:
                        break;
                    case OnceOperation.DO_CARDINALITY_CHECK:
                        if (uniqueKeyMap.put(keyAsString, null) != null) {
                            throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                        }
                        break;
                    case OnceOperation.UNIQUE_CARDINALITY_CHECK:
                        DataValueDescriptor oldValue = uniqueKeyMap.put(keyAsString, nextRowCore.getColumn(subqueryColumnPosition));
                        if (oldValue != null && !oldValue.equals(nextRowCore.getColumn(subqueryColumnPosition))) {
                            throw StandardException.newException(SQLState.LANG_SCALAR_SUBQUERY_CARDINALITY_VIOLATION);
                        }
                        break;
                }
            }
        } finally {
            subquerySource.close();
        }
    }

    private KeyEncoder getKeyEncoder(int[] pkCols, ExecRow execRowDefinition) throws StandardException {
        DataHash dataHash;
        int[] keyColumns = new int[pkCols.length];
        for(int i=0;i<keyColumns.length;i++){
            keyColumns[i] = pkCols[i] -1;
        }
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(SYSTABLESRowFactory.CURRENT_TABLE_VERSION, true).getSerializers(execRowDefinition);
        dataHash = BareKeyHash.encoder(keyColumns,null, SpliceKryoRegistry.getInstance(),serializers);
        return new KeyEncoder(NoOpPrefix.INSTANCE,dataHash,NoOpPostfix.INSTANCE);
    }

    private int getColumnPosition(ExecRow execRow, int[] columnPositions) {
        int[] p = new int[execRow.size()];
        for(int i = 0; i < p.length; ++i) {
            p[i] = 0;
        }

        for (int position : columnPositions) {
            p[position-1] = 1;
        }

        for (int i = 0; i < p.length; ++i) {
            if (p[i] == 0) {
                return i+1;
            }
        }
        assert false;
        return -1;
    }
}
