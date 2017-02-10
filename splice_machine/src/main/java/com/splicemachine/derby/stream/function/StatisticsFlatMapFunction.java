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
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.impl.sql.execute.StatisticsRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.utils.StatisticsAdmin;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StatisticsFlatMapFunction
    extends SpliceFlatMapFunction<ScalarAggregateOperation, Iterator<LocatedRow>, LocatedRow> {
    private static final long serialVersionUID = 844136943916989111L;
    protected boolean initialized;
    protected StatisticsRow statisticsRow;
    protected long conglomId;
    protected int[] columnPositionMap;
    protected ExecRow template;

    public StatisticsFlatMapFunction() {
    }

    public StatisticsFlatMapFunction(long conglomId, int[] columnPositionMap, ExecRow template) {
        assert columnPositionMap != null:"columnPositionMap is null";
        this.conglomId = conglomId;
        this.columnPositionMap = columnPositionMap;
        this.template = template;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomId);
        ArrayUtil.writeIntArray(out,columnPositionMap);
        out.writeObject(template);
    }

    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
        conglomId = in.readLong();
        columnPositionMap = ArrayUtil.readIntArray(in);
        template = (ExecRow) in.readObject();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<LocatedRow> call(Iterator<LocatedRow> locatedRows) throws Exception {
        List<LocatedRow> rows;
        long rowCount = 0l;
        long rowWidth = 0l;
        while (locatedRows.hasNext()) {
            LocatedRow locatedRow = locatedRows.next();
            if (!initialized) {
                statisticsRow = new StatisticsRow(locatedRow.getRow());
                initialized = true;
            }
            rowWidth += locatedRow.getRow().getRowSize();
            rowCount++;
            statisticsRow.setExecRow(locatedRow.getRow());
        }
        if (statisticsRow!=null) {
            int meanRowWidth = (int) ( ((double) rowWidth)/ ((double) rowCount));
            ItemStatistics[] itemStatistics = statisticsRow.getItemStatistics();
            rows = new ArrayList<>(itemStatistics.length+1);
            for(int i=0;i<itemStatistics.length;i++){
                if(itemStatistics[i]==null)
                    continue;
                rows.add(new LocatedRow(StatisticsAdmin.generateRowFromStats(conglomId,SITableScanner.regionId.get(),columnPositionMap[i],itemStatistics[i])));
            }
            rows.add(new LocatedRow(StatisticsAdmin.generateRowFromStats(conglomId,SITableScanner.regionId.get(),rowCount,rowCount*((long)meanRowWidth),meanRowWidth)));
            return rows.iterator();
        } else {
            rows = new ArrayList<>(columnPositionMap.length);
            for (int i = 0; i<columnPositionMap.length;i++) {
                if (columnPositionMap[i] != -1 && template.getColumn(columnPositionMap[i]) !=null)
                rows.add(new LocatedRow(StatisticsAdmin.generateRowFromStats(conglomId, SITableScanner.regionId.get(), columnPositionMap[i], new ColumnStatisticsImpl(template.getColumn(columnPositionMap[i])) )));
            }
            rows.add(new LocatedRow(
                    StatisticsAdmin.generateRowFromStats(conglomId,SITableScanner.regionId.get(),0,0,0)));
            return rows.iterator();
        }
    }
}
