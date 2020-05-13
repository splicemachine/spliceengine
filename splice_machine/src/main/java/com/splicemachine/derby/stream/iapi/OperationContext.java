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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.ActivationHolder;
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
    String getStatusDirectory();

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
        COLLECT_STATS("Collect Statistics (Table %s)"),
        EXPAND("Emit Multiple Rows For Distinct"),
        AGGREGATE("Aggregate Function"),
        READ_KAFKA_TOPIC("Read Kafka Topic");

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
    ActivationHolder getActivationHolder();
}
