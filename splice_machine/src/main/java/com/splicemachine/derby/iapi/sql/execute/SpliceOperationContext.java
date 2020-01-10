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

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.txn.TxnView;


/**
 * Represents the context of a SpliceOperation stack.
 * <p/>
 * This is primarily intended to ease the initialization interface by providing a single
 * wrapper object, instead of 400 different individual elements.
 *
 * @author Scott Fines
 *         Created: 1/18/13 9:18 AM
 */
public class SpliceOperationContext{
    private final GenericStorablePreparedStatement preparedStatement;
    private final Activation activation;
    private TxnView txn;
    private SConfiguration config;

    public SpliceOperationContext(Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  TxnView txn,
                                  SConfiguration config){
        this.activation=activation;
        this.preparedStatement=preparedStatement;
        this.txn=txn;
        this.config = config;
    }

    public GenericStorablePreparedStatement getPreparedStatement(){
        return preparedStatement;
    }

    public Activation getActivation(){
        return activation;
    }

    public TxnView getTxn(){
        return txn;
    }

    public static SpliceOperationContext newContext(Activation a){
        return newContext(a,null); //TODO -sf- make this return a configuration
    }

    public static SpliceOperationContext newContext(Activation a,TxnView txn){
        if(txn==null){
            TransactionController te=a.getLanguageConnectionContext().getTransactionExecute();
            txn=((SpliceTransactionManager)te).getRawTransaction().getActiveStateTxn();
        }
        return new SpliceOperationContext(a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                txn,
                EngineDriver.driver().getConfiguration());
    }

    public SConfiguration getSystemConfiguration(){
        return config;
    }

    public LanguageConnectionContext getLanguageConnectionContext(){
        return activation.getLanguageConnectionContext();
    }
}
