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

package com.splicemachine.si.api.filter;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataScan;
import java.io.IOException;

/**
 * Manages lifecycle and setup for Transactional reads.
 *
 * @author Scott Fines
 *         Date: 2/13/14
 */
public interface TransactionReadController{

    void preProcessGet(DataGet get) throws IOException;

    void preProcessScan(DataScan scan) throws IOException;

    /**
     * Create a DDLFilter for tracking the visibility of (tentative) DDL operations for DML operations
     *
     * @param txn the ddl transaction
     * @return Object that tracks visibility
     */
    DDLFilter newDDLFilter(Txn txn) throws IOException;
}
