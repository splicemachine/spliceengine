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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.BadRecordsRecorder;
import com.splicemachine.pipeline.api.RecordingContext;
import com.splicemachine.si.api.txn.TxnView;
import java.io.Externalizable;
import java.io.IOException;

/**
 * Created by jleach on 4/17/15.
 */
public interface OperationContext<Op extends SpliceOperation> extends Externalizable, RecordingContext {
    void prepare();
    void reset();
    Op getOperation();
    Activation getActivation();

    void recordJoinedLeft();
    void recordJoinedRight();

    long getRecordsRead();
    long getRecordsFiltered();
    long getRecordsWritten();
    long getRetryAttempts();
    long getRegionTooBusyExceptions();

    BadRecordsRecorder getBadRecordsRecorder();

    boolean isPermissive();
    void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold);
    boolean isFailed();
    long getBadRecords();
    String getBadRecordFileName();

    enum Scope{
        READ_TEXT_FILE("Read File"),
        PARSE_FILE("Parse File"),
        SORT_KEYER("Prepare Keys"),
        GROUP_AGGREGATE_KEYER(SORT_KEYER.displayName()),
        REDUCE("Reduce"),
        READ("Read Values"),
        SORT("Sort Records"),
        WINDOW("Window Function"),
        READ_SORTED("Read Sorted Values"),
        ROLLUP("Rollup"),
        EXECUTE("Execute"),
        FINALIZE("Finalize"),
        DISTINCT("Find Distinct Values"),
        SUBTRACT("Subtract/Except Values"),
        INTERSECT("Intersect Values"),
        SHUFFLE("Shuffle/Sort Data"),
        LOCATE("Locate Rows"),
        COLLECT_STATS("Collect Statistics (Table %s)");

        private final String stringValue;

        Scope(String stringValue){
            this.stringValue=stringValue;
        }

        public String displayName(){
           return stringValue;
        }
    }

    void pushScope(String displayName);
    void pushScope();
    void pushScopeForOp(Scope scope);
    void pushScopeForOp(String scope);
    void popScope();

    TxnView getTxn();
    OperationContext getClone() throws IOException, ClassNotFoundException;
}
