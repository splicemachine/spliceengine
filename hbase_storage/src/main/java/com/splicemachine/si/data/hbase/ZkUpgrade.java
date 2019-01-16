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
package com.splicemachine.si.data.hbase;


import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Created by jyuan on 7/17/17.
 */
public class ZkUpgrade {
    private static final Logger LOG = Logger.getLogger(ZkUpgrade.class);
    private static final String OLD_TRANSACTIONS_NODE = "/transactions/v1transactions";

    public static long getOldTransactions(SConfiguration conf) throws IOException {
        try {
            String spliceRootPath = conf.getSpliceRootPath();
            byte[] bytes = ZkUtils.getData(spliceRootPath + OLD_TRANSACTIONS_NODE);
            long txn =  bytes != null ? Bytes.toLong(bytes) : 0;
            SpliceLogUtils.info(LOG, "Max V1 format transaction = %d", txn);

            return txn;
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException.NoNodeException) {
                SpliceLogUtils.info(LOG, "No transaction is encoded in V1 format");
                return 0;
            }
            else throw e;
        }
    }
}
