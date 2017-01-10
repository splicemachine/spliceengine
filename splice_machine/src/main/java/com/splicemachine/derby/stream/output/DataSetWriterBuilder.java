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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.si.api.txn.Txn;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface DataSetWriterBuilder{

    DataSetWriter build() throws StandardException;

    DataSetWriterBuilder destConglomerate(long heapConglom);

    DataSetWriterBuilder txn(Txn txn);

    DataSetWriterBuilder operationContext(OperationContext operationContext);

    DataSetWriterBuilder skipIndex(boolean skipIndex);

    Txn getTxn();

    byte[] getDestinationTable();

    TableWriter buildTableWriter() throws StandardException;
}
