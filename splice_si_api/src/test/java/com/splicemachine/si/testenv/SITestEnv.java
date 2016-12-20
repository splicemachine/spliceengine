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

package com.splicemachine.si.testenv;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.txn.TransactionStore;
import com.splicemachine.storage.Partition;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;

public interface SITestEnv extends SITestDataEnv{

    void initialize() throws IOException;

    String getPersonTableName();

    Clock getClock();

    TransactionStore getTxnStore();

    TimestampSource getTimestampSource();

    Partition getPersonTable(TestTransactionSetup tts) throws IOException;

    Partition getPartition(String name, TestTransactionSetup tts) throws IOException;

    PartitionFactory getTableFactory();

    void createTransactionalTable(byte[] tableNameBytes) throws IOException;
}
