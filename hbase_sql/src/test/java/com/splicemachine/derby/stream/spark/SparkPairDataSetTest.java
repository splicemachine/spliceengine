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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.AbstractPairDataSetTest;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import org.junit.Ignore;

/**
 * Created by jleach on 4/15/15.
 */
@Ignore
public class SparkPairDataSetTest extends AbstractPairDataSetTest{

    @Override
    protected PairDataSet<ExecRow, ExecRow> getTenRows() {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(tenRows));
    }

    @Override
    protected PairDataSet<ExecRow, ExecRow> getEvenRows() {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(evenRows));
    }
}