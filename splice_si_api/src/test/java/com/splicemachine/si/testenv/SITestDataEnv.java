package com.splicemachine.si.testenv;

import com.splicemachine.si.api.data.*;
import com.splicemachine.storage.DataFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public interface SITestDataEnv{

    DataFilterFactory getFilterFactory();

    ExceptionFactory getExceptionFactory();

    OperationStatusFactory getOperationStatusFactory();

    TxnOperationFactory getOperationFactory();

    OperationFactory getBaseOperationFactory();
}
