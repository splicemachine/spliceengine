package com.splicemachine.si.testsetup;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.impl.HTxnOperationFactory;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.HFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class HBaseDataEnv implements SITestDataEnv{
    private final TxnOperationFactory txnOperationFactory;

    public HBaseDataEnv(){
        this.txnOperationFactory = new HTxnOperationFactory(HExceptionFactory.INSTANCE);
    }

    @Override
    public DataFilterFactory getFilterFactory(){
        return HFilterFactory.INSTANCE;
    }

    @Override
    public ExceptionFactory getExceptionFactory(){
        return HExceptionFactory.INSTANCE;
    }

    @Override
    public OperationStatusFactory getOperationStatusFactory(){
        return HOperationStatusFactory.INSTANCE;
    }

    @Override
    public TxnOperationFactory getOperationFactory(){
        return txnOperationFactory;
    }
}
