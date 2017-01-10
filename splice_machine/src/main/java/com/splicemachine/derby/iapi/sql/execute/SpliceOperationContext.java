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

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.txn.Txn;


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
    private Txn txn;
    private SConfiguration config;

    public SpliceOperationContext(Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  Txn txn,
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

    public Txn getTxn(){
        return txn;
    }

    public static SpliceOperationContext newContext(Activation a){
        return newContext(a,null); //TODO -sf- make this return a configuration
    }

    public static SpliceOperationContext newContext(Activation a,Txn txn){
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
