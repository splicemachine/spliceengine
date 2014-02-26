package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import java.io.Externalizable;
import java.io.IOException;

/**
 * Wrapper interface for DMLWrite information (such as activation-related stuff, etc.)
 *
 * Using this allows for better testability (at a slight cost of an extra abstraction)
 *
 * @author Scott Fines
 * Created on: 10/4/13
 */
interface DMLWriteInfo extends Externalizable {

    void initialize(SpliceOperationContext opCtx);

    ConstantAction getConstantAction();

    FormatableBitSet getPkColumns();

    int[] getPkColumnMap();

    long getConglomerateId();

		SpliceObserverInstructions buildInstructions(SpliceOperation operation);

		ResultDescription getResultDescription();
}
