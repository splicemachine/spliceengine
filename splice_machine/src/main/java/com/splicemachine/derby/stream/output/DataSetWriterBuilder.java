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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface DataSetWriterBuilder{

    DataSetWriter build() throws StandardException;

    DataSetWriterBuilder destConglomerate(long heapConglom);

    DataSetWriterBuilder txn(TxnView txn);

    DataSetWriterBuilder token(byte[] token);

    DataSetWriterBuilder operationContext(OperationContext operationContext);

    DataSetWriterBuilder skipIndex(boolean skipIndex);

    TxnView getTxn();

    byte[] getToken();

    byte[] getDestinationTable();

    TableWriter buildTableWriter() throws StandardException;
}
