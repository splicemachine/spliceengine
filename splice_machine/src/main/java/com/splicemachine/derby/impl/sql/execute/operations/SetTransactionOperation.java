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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * This is a wrapper class which invokes the Execution-time logic for
 * SET TRANSACTION statements. The real Execution-time logic lives inside the
 * executeConstantAction() method of the Execution constant.
 */

@SuppressFBWarnings(value="SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION", justification="Serialization"+
        "of this class is a mistake, but we inherit externalizability from SpliceBaseOperation")
public class SetTransactionOperation extends MiscOperation{
    /**
     * Construct a SetTransactionResultSet
     *
     * @param activation Describes run-time environment.
     */
    public SetTransactionOperation(Activation activation) throws StandardException{
        super(activation);
    }

    /**
     * Does this ResultSet cause a commit or rollback.
     *
     * @return Whether or not this ResultSet cause a commit or rollback.
     */
    public boolean doesCommit(){
        return true;
    }
}
