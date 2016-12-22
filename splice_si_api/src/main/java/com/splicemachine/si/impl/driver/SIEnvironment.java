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

package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.txn.TransactionStore;
import com.splicemachine.si.api.txn.TxnFactory;
import com.splicemachine.si.api.txn.TxnLocationFactory;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public interface SIEnvironment{
    PartitionFactory tableFactory();

    ExceptionFactory exceptionFactory();

    SConfiguration configuration();

    TransactionStore txnStore();

    TxnFactory txnFactory();

    OperationStatusFactory statusFactory();

    TimestampSource logicalTimestampSource();

    TxnLocationFactory txnLocationFactory();

    TimestampSource physicalTimestampSource();

    TxnSupplier globalTxnCache();

    TxnOperationFactory operationFactory();

    SIDriver getSIDriver();

    PartitionInfoCache partitionInfoCache();

    DataFilterFactory filterFactory();

    Clock systemClock();

    DistributedFileSystem fileSystem();

    /**
     *
     * Retrieve the appropriate filesystem based on the path and configuration.
     *
     * @param path
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    DistributedFileSystem fileSystem(String path) throws IOException, URISyntaxException;

    OperationFactory baseOperationFactory();

    SnowflakeFactory snowflakeFactory();

}
