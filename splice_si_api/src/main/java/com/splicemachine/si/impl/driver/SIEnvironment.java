/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.impl.driver;

import com.splicemachine.access.api.*;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.rollforward.RollForward;
import com.splicemachine.si.api.server.ClusterHealth;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.store.IgnoreTxnSupplier;
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

    TxnStore txnStore();

    OperationStatusFactory statusFactory();

    TimestampSource timestampSource();

    TxnSupplier txnSupplier();

    IgnoreTxnSupplier ignoreTxnSupplier();

    RollForward rollForward();

    TxnOperationFactory operationFactory();

    SIDriver getSIDriver();

    PartitionInfoCache partitionInfoCache();

    KeepAliveScheduler keepAliveScheduler();

    DataFilterFactory filterFactory();

    Clock systemClock();

    KeyedReadResolver keyedReadResolver();

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

    ClusterHealth clusterHealthFactory();

    FilesystemAdmin filesystemAdmin();

    OldestActiveTransactionTaskFactory oldestActiveTransactionTaskFactory();
}
