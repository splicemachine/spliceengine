package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.si.api.TxnView;

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
    void pushScope(String display);
    void pushScope();
    void pushScopeForOp(String step);
    void popScope();
    void recordBadRecord(String badRecord);
    boolean isPermissive();
    void setPermissive();
    void setFailBadRecordCount(int failBadRecordCount);
    boolean isFailed();
    List<String> getBadRecords();
    byte[] getOperationUUID();

}