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

package com.splicemachine.si.testsetup;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.data.hbase.HOperationStatusFactory;
import com.splicemachine.si.impl.HOperationFactory;
import com.splicemachine.si.impl.SimpleTxnOperationFactory;
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
        this.txnOperationFactory = new SimpleTxnOperationFactory(HExceptionFactory.INSTANCE,HOperationFactory.INSTANCE);
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

    @Override
    public OperationFactory getBaseOperationFactory(){
        return HOperationFactory.INSTANCE;
    }
}
