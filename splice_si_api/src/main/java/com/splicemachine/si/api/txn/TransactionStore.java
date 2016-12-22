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

package com.splicemachine.si.api.txn;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/24/14
 */
public interface TransactionStore extends TxnSupplier{

    /**
     * Write the Txn to underlying storage.
     *
     * @param txn the transaction to write.
     * @throws IOException if something goes wrong trying to write it
     */
    void recordNewTransaction(Txn transaction) throws IOException;

    void rollback(Txn transaction) throws IOException;

    long commit(Txn transaction) throws IOException;

    void elevateTransaction(Txn transaction) throws IOException;

    /**
     * @return a count of the total number of store lookups made since the server last started
     */
    long lookupCount();

    /**
     * @return a count of the total number of transactions elevated since the server last started
     */
    long elevationCount();

    /**
     * @return a count of the total number of writable transactions created since the server last started
     */
    long createdCount();

    /**
     * @return a count of the total number of transaction rollbacks made since the server last started
     */
    long rollbackCount();

    /**
     * @return a count of the total number of transaction commits made since the server last started
     */
    long commitCount();

}
