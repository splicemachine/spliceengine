package com.splicemachine.si.api.txn.lifecycle;

import com.splicemachine.si.api.txn.Txn;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface CannotCommitException{

    long getTxnId();

    Txn.State getActualState();

}
