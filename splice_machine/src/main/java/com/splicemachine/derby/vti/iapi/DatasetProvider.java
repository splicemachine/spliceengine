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

package com.splicemachine.derby.vti.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Created by jleach on 10/7/15.
 */
public interface DatasetProvider {
    /**
     *
     * Processing pipeline supporting in memory and spark datasets.
     *
     * @param dsp
     * @param execRow
     * @return
     * @throws StandardException
     */
     DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp,ExecRow execRow) throws StandardException;

    /**
     *
     * Dynamic MetaData used to dynamically bind a function.
     *
     * @return
     */
    ResultSetMetaData getMetaData() throws SQLException;

    OperationContext getOperationContext();

}
