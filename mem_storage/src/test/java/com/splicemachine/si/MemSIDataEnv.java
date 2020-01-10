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

package com.splicemachine.si;

import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.impl.MOpStatusFactory;
import com.splicemachine.si.impl.MOperationFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
import com.splicemachine.si.impl.data.MExceptionFactory;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.MFilterFactory;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class MemSIDataEnv implements SITestDataEnv{
    private final MOperationFactory opFactory;
    private TxnOperationFactory txnOpFactory;

    public MemSIDataEnv(){
        this.opFactory=new MOperationFactory(new IncrementingClock());
        this.txnOpFactory = new SimpleTxnOperationFactory(MExceptionFactory.INSTANCE, opFactory);
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

    @Override
    public OperationFactory getBaseOperationFactory(){
        return opFactory;
    }
}
