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
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.txn.TxnView;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Created by jyuan on 6/1/17.
 */
public class BulkDeleteRowIndexGenerationFunction extends RowAndIndexGenerator {

    private String bulkDeleteDirectory;
    private int[] colMap;
    private Map<Long, int[]> indexColMap;

    public BulkDeleteRowIndexGenerationFunction(){}

    public BulkDeleteRowIndexGenerationFunction(OperationContext operationContext,
                                                TxnView txn,
                                                long heapConglom,
                                                ArrayList<DDLMessage.TentativeIndex> tentativeIndices,
                                                String bulkDeleteDirectory,
                                                int[] colMap) {

        super(operationContext, txn, heapConglom, tentativeIndices);
        this.bulkDeleteDirectory = bulkDeleteDirectory;
        this.colMap = colMap;
    }

    @Override
    public Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> call(ExecRow locatedRow) throws Exception {
        init();

        ArrayList<Tuple2<Long,Tuple2<byte[], byte[]>>> list = new ArrayList();

        RowLocation location = (RowLocation)locatedRow.getColumn(locatedRow.nColumns()).getObject();
        list.add(new Tuple2<>(heapConglom,new Tuple2<>(location.getBytes(), new byte[0])));

        for (int i = 0; i< indexTransformFunctions.length; i++) {
            ExecRow indexRow = getIndexRow(indexTransformFunctions[i], locatedRow);
            Long indexConglomerate = indexTransformFunctions[i].getIndexConglomerateId();
            KVPair indexKVPair = indexTransformFunctions[i].call(indexRow);
            if (indexKVPair != null) // Supports Null and Default Expression Indexes
                list.add(new Tuple2<>(indexConglomerate, new Tuple2<>(indexKVPair.getRowKey(), new byte[0])));
        }

        return list.iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(bulkDeleteDirectory);
        ArrayUtil.writeIntArray(out, colMap);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        bulkDeleteDirectory = in.readUTF();
        colMap = ArrayUtil.readIntArray(in);
    }

    private void init() {
        if (!initialized)
        {
            indexTransformFunctions = new IndexTransformFunction[tentativeIndices.size()];
            List<Integer> cols = new ArrayList<>();
            if (colMap != null && colMap.length > 0) {
                for (int i = 0; i < colMap.length; ++i) {
                    cols.add(colMap[i]);
                }
                indexColMap = new HashMap<>();
                int n = 0;
                for (DDLMessage.TentativeIndex index : tentativeIndices) {
                    Long conglom = index.getIndex().getConglomerate();
                    List<Integer> indexColsToMainColMapList = index.getIndex().getIndexColsToMainColMapList();
                    List<Integer> indexColToScanRowList = new ArrayList<>();
                    for (int i = 0; i < indexColsToMainColMapList.size(); ++i) {
                        indexColToScanRowList.add(i, cols.indexOf(indexColsToMainColMapList.get(i)));
                    }
                    Collections.sort(indexColToScanRowList);
                    int[] indexColToScanRowMap = new int[indexColToScanRowList.size()];
                    for (int i = 0; i < indexColToScanRowMap.length; ++i) {
                        indexColToScanRowMap[i] = indexColToScanRowList.get(i);
                    }
                    indexColMap.put(conglom, indexColToScanRowMap);
                    indexTransformFunctions[n] = new IndexTransformFunction(index);
                    n++;
                }
            }
            initialized = true;
        }
    }

    /**
     * Strip off all non-index columns from a main table row
     */
    private ExecRow getIndexRow(IndexTransformFunction indexTransformFunction, ExecRow execRow) throws StandardException {
        List<Integer> indexColToMainCol = indexTransformFunction.getIndexColsToMainColMapList();
        Long conglom = indexTransformFunction.getIndexConglomerateId();
        int[] indexColToScanRowMap = indexColMap.get(conglom);
        ExecRow row = new ValueRow(indexColToMainCol.size());
        int col = 1;
        for (Integer n : indexColToScanRowMap) {
            row.setColumn(col, execRow.getColumn(n+1));
            col++;
        }
        RowLocation location = (RowLocation)execRow.getColumn(execRow.nColumns()).getObject();
        row.setKey(location.getBytes());
        return row;
    }
}
