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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.TupleFilter;
import com.splicemachine.db.iapi.types.BooleanDataValue;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLBoolean;

/**
 * Created by yxia on 11/10/19.
 */
public class ColumnStatsFilter implements TupleFilter {
    int columnId;
    DataValueFactory dataValueFactory = null;

    BooleanDataValue	trueValue;
    BooleanDataValue	falseValue;

    public ColumnStatsFilter(int columnId) {
        this.columnId = columnId;
    }

    public void init(ExecRow parameters) throws StandardException {
    }

    public BooleanDataValue execute(ExecRow currentRow)
            throws StandardException
    {
		/* 3rd column is PROVIDERID (UUID - char(36)) */
        DataValueDescriptor col = currentRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.COLUMNID);
        int colId = col.getInt();
        if (colId == columnId) {
            return getTrueValue();
        } else {
            return getFalseValue();
        }
    }

    private BooleanDataValue getTrueValue() throws StandardException
    {
        if (trueValue == null)
        {
            trueValue = new SQLBoolean(true);
        }

        return trueValue;
    }

    private BooleanDataValue getFalseValue() throws StandardException
    {
        if (falseValue == null)
        {
            falseValue = new SQLBoolean(false);
        }

        return falseValue;
    }
}
