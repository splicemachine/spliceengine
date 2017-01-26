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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.junit.BeforeClass;

/**
 * Created by jleach on 8/5/16.
 */
public class AbstractKeyStatisticsImplTest {
    protected static NonUniqueKeyStatisticsImpl impl;
    protected static ExecRow minRow;
    protected static ExecRow maxRow;
    protected static ExecRow row2000;
    protected static ExecRow row6000;

    public static void init() throws StandardException {
        ExecRow execRow = new ValueRow(3);
        execRow.setColumn(1, new SQLDouble());
        execRow.setColumn(2, new SQLVarchar());
        execRow.setColumn(3, new SQLLongint());
        impl = new NonUniqueKeyStatisticsImpl(execRow);
        for (int i = 1; i <= 10000; i++) {
            ExecRow clone = new ValueRow(3);
            clone.setColumn(1, new SQLDouble(i));
            clone.setColumn(2, new SQLVarchar("" + i));
            clone.setColumn(3, new SQLLongint(i));
            impl.update(clone);
        }
        maxRow = new ValueRow(3);
        maxRow.setRowArray(new DataValueDescriptor[]{
                new SQLDouble(10000),
                new SQLVarchar(""+10000),
                new SQLLongint(10000)
        });
        minRow = new ValueRow(3);
        minRow.setRowArray(new DataValueDescriptor[]{
                new SQLDouble(1),
                new SQLVarchar(""+1),
                new SQLLongint(1)
        });

        row2000 = execRow.getClone();
        row2000.setRowArray(new DataValueDescriptor[]{
                new SQLDouble(2000),
                new SQLVarchar(""+2000),
                new SQLLongint(2000)
        });

        row6000 = execRow.getClone();
        row6000.setRowArray(new DataValueDescriptor[]{
                new SQLDouble(6000),
                new SQLVarchar(""+6000),
                new SQLLongint(6000)
        });
    }
}
