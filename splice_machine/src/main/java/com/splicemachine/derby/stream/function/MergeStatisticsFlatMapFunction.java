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
 *
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.impl.sql.execute.StatisticsRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.StatisticsAdmin;
import com.splicemachine.derby.utils.StatisticsOperation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MergeStatisticsFlatMapFunction
    extends SpliceFlatMapFunction<StatisticsOperation, Iterator<ExecRow>, MergeStatisticsHolder> {

    public MergeStatisticsFlatMapFunction() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<MergeStatisticsHolder> call(Iterator<ExecRow> locatedRows) throws Exception {
        MergeStatisticsHolder msh = new MergeStatisticsHolder();
        while (locatedRows.hasNext()) {
            msh.merge(locatedRows.next());
        }
        return Arrays.asList(msh).iterator();
    }
}
