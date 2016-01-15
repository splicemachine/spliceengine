package com.splicemachine.si;

import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.MTxnOperationFactory;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.impl.data.light.LDataLib;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.MFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class MemSIDataEnv implements SITestDataEnv{
    private SDataLib dataLib = new LDataLib();
    private TxnOperationFactory txnOpFactory;

    public MemSIDataEnv(){
        this.txnOpFactory = new MTxnOperationFactory(dataLib,new IncrementingClock(),MExceptionFactory.INSTANCE);
    }

    @Override
    public SDataLib getDataLib(){
        return dataLib;
    }

    @Override
    public DataFilterFactory getFilterFactory(){
        return MFilterFactory.INSTANCE;
    }

    @Override
    public ExceptionFactory getExceptionFactory(){
        return MExceptionFactory.INSTANCE;
    }

    @Override
    public OperationStatusFactory getOperationStatusFactory(){
        return MOpStatusFactory.INSTANCE;
    }

    @Override
    public TxnOperationFactory getOperationFactory(){
        return txnOpFactory;
    }
}
