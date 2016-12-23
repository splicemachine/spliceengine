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

package com.splicemachine.si.api.server;

import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import java.io.IOException;

/**
 * The primary interface to the transaction module. This interface has the most burdensome generic signature so it is
 * only exposed in places where it is needed.
 */
public interface Transactor{

    boolean processRecord(Partition table,Record record) throws IOException;

    // Do we need this?
    MutationStatus[] processRecordBatch(Partition table,Record[] records) throws IOException;

    MutationStatus[] processRecordBatch(Partition table,
                                    Record[] records,
                                    Txn txn,
                                    IsolationLevel isolationLevel,
                                    ConstraintChecker constraintChecker) throws IOException;
}
