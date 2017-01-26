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

package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;

import java.io.Externalizable;

/**
 * Wrapper interface for DMLWrite information (such as activation-related stuff, etc.)
 *
 * Using this allows for better testability (at a slight cost of an extra abstraction)
 *
 * @author Scott Fines
 * Created on: 10/4/13
 */
public interface DMLWriteInfo extends Externalizable {

    void initialize(SpliceOperationContext opCtx) throws StandardException;

    ConstantAction getConstantAction();

    FormatableBitSet getPkColumns();

    int[] getPkColumnMap();

    long getConglomerateId();

    ResultDescription getResultDescription();

}
