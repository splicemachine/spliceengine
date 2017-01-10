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

package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Record;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class NoOpConstraintChecker implements ConstraintChecker{
    private final OperationStatusFactory opFactory;

    public NoOpConstraintChecker(OperationStatusFactory opFactory){
        this.opFactory=opFactory;
    }

    @Override
    public MutationStatus checkConstraint(Record mutation, Record existingRow) throws IOException{
        return opFactory.success();
    }
}
