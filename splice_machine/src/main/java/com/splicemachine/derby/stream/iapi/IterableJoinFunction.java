/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

/**
 * Created by jleach on 5/4/15.
 */
public interface IterableJoinFunction {
    public boolean hasNext();
    public ExecRow getRightRow();
    public ExecRow getLeftRow();
    public RowLocation getLeftRowLocation();
    public LocatedRow getLeftLocatedRow();
    public boolean wasRightOuterJoin();
    public ExecutionFactory getExecutionFactory();
    public int getNumberOfColumns();
    public void setCurrentLocatedRow(LocatedRow locatedRow);
    public int getResultSetNumber();
    public OperationContext getOperationContext();

}
