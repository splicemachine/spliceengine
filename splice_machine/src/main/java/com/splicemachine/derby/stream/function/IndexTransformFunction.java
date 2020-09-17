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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.db.impl.sql.execute.BaseExecutableIndexExpression;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.kvpair.KVPair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 10/16/15.
 */
public class IndexTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,ExecRow,KVPair> {
    private static Logger LOG = Logger.getLogger(IndexTransformFunction.class);
    private boolean initialized;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] projectedMapping;
    private ExecRow indexRow;
    private boolean isSystemTable;

    private transient IndexTransformer transformer;

    public IndexTransformFunction() {
        super();
    }

    public IndexTransformFunction(DDLMessage.TentativeIndex tentativeIndex,
                                  List<Integer> baseColumnMapList,
                                  boolean isSystemTable) {

        this.tentativeIndex = tentativeIndex;
        this.isSystemTable = isSystemTable;
        List<Integer> actualList = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        projectedMapping = new int[actualList.size()];
        for (int i =0; i<projectedMapping.length;i++) {
            projectedMapping[i] = baseColumnMapList.indexOf(actualList.get(i));
        }
    }

    public IndexTransformFunction(DDLMessage.TentativeIndex tentativeIndex) {
        this.tentativeIndex = tentativeIndex;
        List<Integer> actualList = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        List<Integer> sortedList = new ArrayList<>(actualList);
        Collections.sort(sortedList);
        projectedMapping = new int[sortedList.size()];
        for (int i =0; i<projectedMapping.length;i++) {
            projectedMapping[i] = sortedList.indexOf(actualList.get(i));
        }
    }

    @Override
    public KVPair call(ExecRow execRow) throws Exception {
        if (!initialized)
            init(execRow);
        ExecRow misMatchedRow = execRow;
        int numIndexExprs = transformer.getNumIndexExprs();
        if (numIndexExprs <= 0) {
            for (int i = 0; i < projectedMapping.length; i++) {
                indexRow.setColumn(i + 1, misMatchedRow.getColumn(projectedMapping[i] + 1));
            }
        } else {
            int maxNumCols = transformer.getMaxBaseColumnPosition();
            ExecRow expandedRow = new ValueRow(maxNumCols);
            int[] usedBaseColumns = getIndexColsToMainColMapList().stream().mapToInt(i->i).toArray();
            BitSet bitSet = new BitSet();
            for (int ubc : usedBaseColumns) {
                bitSet.set(ubc);
            }
            for (int expandedRowIndex = 1, baseRowIndex = 1; expandedRowIndex <= maxNumCols; expandedRowIndex++) {
                if (bitSet.get(expandedRowIndex)) {
                    expandedRow.setColumn(expandedRowIndex, misMatchedRow.getColumn(baseRowIndex));
                    baseRowIndex++;
                }
            }
            for (int i = 0; i < numIndexExprs; i++) {
                BaseExecutableIndexExpression execExpr = transformer.getExecutableIndexExpression(i);
                execExpr.runExpression(expandedRow, indexRow);
            }
        }
        if (isSystemTable) {
            indexRow.setColumn(indexRow.nColumns(), new HBaseRowLocation(misMatchedRow.getKey()));
        }

        indexRow.setKey(misMatchedRow.getKey());
        KVPair kvPair;
        if (!isSystemTable) {
            kvPair = transformer.writeDirectIndex(indexRow);
        }
        else {
            kvPair = transformer.encodeSystemTableIndex(indexRow);
        }
        return kvPair;
    }

    private void init(ExecRow execRow) throws StandardException {
        transformer = new IndexTransformer(tentativeIndex);
        initialized = true;
        int numIndexColumns = tentativeIndex.getIndex().getDescColumnsCount();
        indexRow = new ValueRow(!isSystemTable ? numIndexColumns : numIndexColumns+1);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] message = tentativeIndex.toByteArray();
        out.writeInt(message.length);
        out.write(message);
        ArrayUtil.writeIntArray(out,projectedMapping);
        out.writeBoolean(isSystemTable);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] message = new byte[in.readInt()];
        in.readFully(message);
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(message);
        projectedMapping= ArrayUtil.readIntArray(in);
        initialized = false;
        isSystemTable = in.readBoolean();
    }

    public long getIndexConglomerateId() {
        return tentativeIndex.getIndex().getConglomerate();
    }

    public List<Integer> getIndexColsToMainColMapList() {
        return tentativeIndex.getIndex().getIndexColsToMainColMapList();
    }
}
