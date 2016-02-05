package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.si.api.txn.TxnView;
import java.io.Externalizable;
import java.util.List;

/**
 * Created by jleach on 4/17/15.
 */
public interface OperationContext<Op extends SpliceOperation> extends Externalizable {
    void prepare();
    void reset();
    Op getOperation();
    Activation getActivation();
    void recordRead();
    void recordFilter();
    void recordWrite();
    void recordJoinedLeft();
    void recordJoinedRight();
    void recordProduced();
    long getRecordsRead();
    long getRecordsFiltered();
    long getRecordsWritten();

    void recordBadRecord(String badRecord);
    boolean isPermissive();
    void setPermissive();
    void setFailBadRecordCount(int failBadRecordCount);
    boolean isFailed();
    List<String> getBadRecords();
    byte[] getOperationUUID();

    enum Scope{
        READ_TEXT_FILE("Read File From Disk"),
        PARSE_FILE("Parse File"),
        SORT_KEYER("Prepare Keys"),
        GROUP_AGGREGATE_KEYER(SORT_KEYER.displayName()),
        REDUCE("Reduce"),
        READ("Read Values"),
        READ_SORTED("Read Sorted Values"),
        ROLLUP("Rollup"),
        EXECUTE("Execute"),
        FINALIZE("Finalize"),
        DISTINCT("Find Distinct Values"),
        SHUFFLE("Shuffle/Sort Data"),
        LOCATE("Locate Rows");

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
}