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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MTransactionTimeout extends IOException implements TransactionTimeoutException{

    public MTransactionTimeout(){
    }

    public MTransactionTimeout(long txnId){
        super("Transaction "+txnId+" has timed out");
    }

    public MTransactionTimeout(String message){
        super(message);
    }

    public MTransactionTimeout(String message,Throwable cause){
        super(message,cause);
    }
}
