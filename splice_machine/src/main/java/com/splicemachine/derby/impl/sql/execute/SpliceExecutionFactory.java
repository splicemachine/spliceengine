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

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.sql.execute.ResultSetFactory;
import com.splicemachine.db.impl.sql.execute.GenericConstantActionFactory;
import com.splicemachine.db.impl.sql.execute.GenericExecutionFactory;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceExecutionFactory extends GenericExecutionFactory {

    private static Logger LOG = Logger.getLogger(SpliceExecutionFactory.class);

    private SpliceGenericResultSetFactory resultSetFactory;
    private SpliceGenericConstantActionFactory genericConstantActionFactory;

    public SpliceExecutionFactory() {
        super();
        SpliceLogUtils.trace(LOG, "instantiating ExecutionFactory");
    }

    @Override
    public ResultSetFactory getResultSetFactory() {
        SpliceLogUtils.trace(LOG, "getResultSetFactory");
        if (resultSetFactory == null)
            resultSetFactory = new SpliceGenericResultSetFactory();
        return resultSetFactory;
    }

    @Override
    public GenericConstantActionFactory getConstantActionFactory() {
        if (genericConstantActionFactory == null) {
            genericConstantActionFactory = newConstantActionFactory();
        }
        return genericConstantActionFactory;
    }

    protected abstract SpliceGenericConstantActionFactory newConstantActionFactory();
}
