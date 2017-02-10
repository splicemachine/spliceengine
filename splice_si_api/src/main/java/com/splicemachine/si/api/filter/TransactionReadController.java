/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.si.api.filter;

import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.EntryPredicateFilter;

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

    TxnFilter newFilterState(ReadResolver readResolver,TxnView txn) throws IOException;

    TxnFilter newFilterStatePacked(ReadResolver readResolver,
                                   EntryPredicateFilter predicateFilter,
                                   TxnView txn,boolean countStar) throws IOException;

    /**
     * Create a DDLFilter for tracking the visibility of (tentative) DDL operations for DML operations
     *
     * @param txn the ddl transaction
     * @return Object that tracks visibility
     */
    DDLFilter newDDLFilter(TxnView txn) throws IOException;
}
