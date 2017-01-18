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

package com.splicemachine.si;

import com.splicemachine.si.api.data.*;
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.testenv.SITestDataEnv;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class MemSIDataEnv implements SITestDataEnv{
    private TxnOperationFactory txnOpFactory;

    public MemSIDataEnv(){
        this.txnOpFactory = new SimpleTxnOperationFactory();
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
