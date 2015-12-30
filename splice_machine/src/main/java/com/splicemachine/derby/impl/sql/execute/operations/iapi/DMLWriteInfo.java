package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
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
