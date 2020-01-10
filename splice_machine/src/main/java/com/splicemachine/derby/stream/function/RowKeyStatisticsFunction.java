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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import scala.Tuple2;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;

public class RowKeyStatisticsFunction <Op extends SpliceOperation>
        extends SpliceFlatMapFunction<Op,Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>>, Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> implements Serializable {

    protected Map<Long, ColumnStatisticsImpl> keyStatisticsMap;
    protected Map<Long, Double> rowSizeMap;
    protected List<DDLMessage.TentativeIndex> tentativeIndexList;
    protected long heapConglom;
    protected boolean initialized;
    protected SQLBlob blob;
    public RowKeyStatisticsFunction() {
    }

    public RowKeyStatisticsFunction(long heapConglom, List<DDLMessage.TentativeIndex> tentativeIndexList) {
        this.heapConglom = heapConglom;
        this.tentativeIndexList = tentativeIndexList;
    }

    @Override
    public Iterator<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> call(Iterator<Tuple2<Long,Tuple2<byte[], byte[]>>> mainAndIndexRows) throws Exception {

        if (!initialized) {
            init();
            initialized = true;
        }
        while (mainAndIndexRows.hasNext()) {
            Tuple2<Long, Tuple2<byte[], byte[]>> t = mainAndIndexRows.next();
            Long conglomId = t._1;
            Tuple2<byte[], byte[]> kvPair = t._2;
            byte[] key = kvPair._1;
            byte[] value = kvPair._2;

            long length = value.length + key.length;
            ColumnStatisticsImpl columnStatistics = keyStatisticsMap.get(conglomId);
            if (columnStatistics == null) {
                columnStatistics = new ColumnStatisticsImpl(new SQLBlob(key));
                keyStatisticsMap.put(conglomId, columnStatistics);
            }
            else {
                columnStatistics.update(new SQLBlob(key));
            }

            Double size = rowSizeMap.get(conglomId);
            if (size == null) {
                size = new Double(0);
            }
            size += length;
            rowSizeMap.put(conglomId, size);
        }
        List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> l = Lists.newArrayList();
        for (Map.Entry<Long, ColumnStatisticsImpl> longColumnStatisticsEntry : keyStatisticsMap.entrySet()) {
            ColumnStatisticsImpl stats = longColumnStatisticsEntry.getValue();
            Double size = rowSizeMap.get(longColumnStatisticsEntry.getKey());
            l.add(new Tuple2<>(longColumnStatisticsEntry.getKey(),new Tuple2<>(size, stats)));
        }
        return l.iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(heapConglom);
        out.writeInt(tentativeIndexList.size());
        for (DDLMessage.TentativeIndex ti: tentativeIndexList) {
            byte[] message = ti.toByteArray();
            out.writeInt(message.length);
            out.write(message);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        heapConglom = in.readLong();
        int iSize = in.readInt();
        tentativeIndexList = new ArrayList<>(iSize);
        for (int i = 0; i< iSize; i++) {
            byte[] message = new byte[in.readInt()];
            in.readFully(message);
            tentativeIndexList.add(DDLMessage.TentativeIndex.parseFrom(message));
        }
    }

    private void init() {
        keyStatisticsMap = new HashMap<>();
        blob = new SQLBlob();
        rowSizeMap = new HashMap<>();
    }
}
