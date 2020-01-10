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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.api.txn.TxnView;

/**
 * Created by jyuan on 10/6/17.
 */
public interface BulkLoadIndexDataSetWriterBuilder {

    BulkLoadIndexDataSetWriterBuilder bulkLoadDirectory(String bulkLoadDirectory);

    BulkLoadIndexDataSetWriterBuilder sampling(boolean sampling);

    BulkLoadIndexDataSetWriterBuilder destConglomerate(long heapConglom);

    BulkLoadIndexDataSetWriterBuilder txn(TxnView txn);

    BulkLoadIndexDataSetWriterBuilder operationContext(OperationContext operationContext);

    BulkLoadIndexDataSetWriterBuilder tentativeIndex(DDLMessage.TentativeIndex tentativeIndex);

    BulkLoadIndexDataSetWriterBuilder indexName(String indexName);

    BulkLoadIndexDataSetWriterBuilder tableVersion(String tableVersion);

    DataSetWriter build() throws StandardException;
}
