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
 *
 */

package com.splicemachine.derby.utils;

import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class ZkUpgradeK2 implements UpgradeK2 {
    private static final Logger LOG = Logger.getLogger(ZkUpgradeK2.class);
    private final String path;
    private static final String K2_NODE = "isK2";

    public ZkUpgradeK2(){
        path =SIDriver.driver().getConfiguration().getSpliceRootPath()+"/" + K2_NODE;
    }

    @Override
    public void setVersion() throws IOException {
        try {
            ZkUtils.create(path, new byte[]{0}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOG.info("Created znode " + K2_NODE);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
