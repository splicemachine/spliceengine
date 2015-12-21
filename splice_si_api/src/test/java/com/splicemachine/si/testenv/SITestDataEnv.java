package com.splicemachine.si.testenv;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.DataFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public interface SITestDataEnv{
    SDataLib getDataLib();

    DataFilterFactory getFilterFactory();

    ExceptionFactory getExceptionFactory();

    OperationStatusFactory getOperationStatusFactory();

    TxnOperationFactory getOperationFactory();

}
