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
 *
 */

package com.splicemachine.derby;

import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.si.api.txn.TxnView;

import java.util.function.Function;

/**
 * @author Scott Fines
 *         Date: 12/20/16
 */
public class TransactionContextUtils{


    public static <T> T operate(TxnView txn,Function<SpliceTransactionResourceImpl,T> action){
        SpliceTransactionResourceImpl impl = null;
        boolean prepared = false;
        try{
            impl = new SpliceTransactionResourceImpl();
            prepared = impl.marshallTransaction(txn);
            return action.apply(impl);
        }catch(Exception ee){
            throw new RuntimeException(ee);
        } finally{
            if(prepared)
                impl.close();
        }
    }
}
