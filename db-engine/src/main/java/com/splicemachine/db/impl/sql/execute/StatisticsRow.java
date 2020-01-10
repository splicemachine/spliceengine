/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
/**
 *
 * Wrapper that updates statistics as data is put in the row format.
 *
 */
public class StatisticsRow extends ValueRow {
    private ItemStatistics[] statistics;

    public StatisticsRow(ExecRow execRow) throws StandardException {
        assert execRow!=null:"ExecRow passed in is null";
        this.setRowArray(execRow.getRowArray());
        statistics = new ItemStatistics[execRow.nColumns()];
        for (int i = 0; i< execRow.nColumns(); i++) {
            DataValueDescriptor dvd = execRow.getColumn(i+1);
            statistics[i] = new ColumnStatisticsImpl(dvd);
        }
    }


    /**
     *
     * Sets the statistical values as well.
     *
     * @param position
     * @param col
     */
    @Override
    public void setColumn(int position, DataValueDescriptor col) {
        statistics[position-1].update(col);
    }

    public void setExecRow(ExecRow execRow) throws StandardException {
        for (int i = 1; i<= execRow.nColumns(); i++) {
            setColumn(i,execRow.getColumn(i));
        }
    }

    public ItemStatistics[] getItemStatistics() {
        return statistics;
    }
}
