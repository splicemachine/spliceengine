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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;

/**
 * Provider of a name to be used as a displayable short description
 * of an operation or sub operation within a running or completed query.
 * For example, sub types of: {@link SpliceBaseOperation} or implementors
 * of {@link ConstantAction} will typically provide a name that can be
 * used in the Spark job admin pages to identify the operation
 * within a job or a stage.
 */
public interface ScopeNamed {
    String getScopeName();
}
