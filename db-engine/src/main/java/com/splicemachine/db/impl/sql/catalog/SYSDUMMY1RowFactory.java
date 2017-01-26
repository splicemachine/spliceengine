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

package com.splicemachine.db.impl.sql.catalog;

import java.sql.Types;

import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;

/**
 * Factory for creating a SYSDUMMY1 row.
 *
 * @version 0.01
 *
 */

public class SYSDUMMY1RowFactory extends CatalogRowFactory
{
    protected static final int SYSDUMMY1_COLUMN_COUNT = 1;

    private static final String[] uuids =
        {
            "c013800d-00f8-5b70-bea3-00000019ed88", // catalog UUID
            "c013800d-00f8-5b70-fee8-000000198c88"  // heap UUID.
        };

    /*
     *	CONSTRUCTORS
     */
    public SYSDUMMY1RowFactory(UUIDFactory uuidf,
                               ExecutionFactory ef,
                               DataValueFactory dvf)
    {
        super(uuidf,ef,dvf);

        initInfo(SYSDUMMY1_COLUMN_COUNT, "SYSDUMMY1",
                 null, null, uuids);
    }


    /**
     * Make a SYSDUMMY1 row
     *
     *
     * @return	Row suitable for inserting into SYSSTATISTICS.
     *
     * @exception   StandardException thrown on failure
     */

    public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
        throws StandardException
    {
        ExecRow row = getExecutionFactory().getValueRow(SYSDUMMY1_COLUMN_COUNT);

        row.setColumn(1, new SQLChar("Y"));
        return row;
    }

    public TupleDescriptor buildDescriptor(
        ExecRow 			row,
        TupleDescriptor    parentDesc,
        DataDictionary 	dd)
        throws StandardException

    {
        return null;
    }

    /**
     * Builds a list of columns suitable for creating this Catalog.
     *
     *
     * @return array of SystemColumn suitable for making this catalog.
     */
    public SystemColumn[] buildColumnList()
    {
        return new SystemColumn[] {
            SystemColumnImpl.getColumn("IBMREQD", Types.CHAR, true, 1)
        };
    }


}
