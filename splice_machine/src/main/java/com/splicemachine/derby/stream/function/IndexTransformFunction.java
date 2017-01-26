/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.kvpair.KVPair;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 10/16/15.
 */
public class IndexTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,KVPair> {
    private boolean initialized;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] indexFormatIds;
    private int[] projectedMapping;

    private transient IndexTransformer transformer;

    public IndexTransformFunction() {
        super();
    }

    public IndexTransformFunction(DDLMessage.TentativeIndex tentativeIndex, int[] indexFormatIds) {
        this.tentativeIndex = tentativeIndex;
        this.indexFormatIds = indexFormatIds;
        List<Integer> actualList = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        List<Integer> sortedList = new ArrayList<>(actualList);
        Collections.sort(sortedList);
        projectedMapping = new int[sortedList.size()];
        for (int i =0; i<projectedMapping.length;i++) {
            projectedMapping[i] = sortedList.indexOf(actualList.get(i));
        }

    }

    @Override
    public KVPair call(LocatedRow locatedRow) throws Exception {
        if (!initialized)
            init();
        ExecRow misMatchedRow = locatedRow.getRow();
        ExecRow row = new ValueRow(misMatchedRow.nColumns());
        for (int i = 0; i<projectedMapping.length;i++) {
              row.setColumn(i+1,misMatchedRow.getColumn(projectedMapping[i]+1));
        }
        locatedRow.setRow(row);
        return transformer.writeDirectIndex(locatedRow);
    }

    private void init() {
        transformer = new IndexTransformer(tentativeIndex);
        initialized = true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] message = tentativeIndex.toByteArray();
        out.writeInt(message.length);
        out.write(message);
        ArrayUtil.writeIntArray(out,indexFormatIds);
        ArrayUtil.writeIntArray(out,projectedMapping);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] message = new byte[in.readInt()];
        in.readFully(message);
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(message);
        indexFormatIds= ArrayUtil.readIntArray(in);
        projectedMapping= ArrayUtil.readIntArray(in);
        initialized = false;
    }
}
