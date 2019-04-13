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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.lifecycle.EngineLifecycleService;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.lifecycle.MasterLifecycle;
import com.splicemachine.olap.OlapServer;
import com.splicemachine.olap.OlapServerSubmitter;
import com.splicemachine.pipeline.InitializationCompleted;
import com.splicemachine.si.data.hbase.coprocessor.CoprocessorUtils;
import com.splicemachine.si.data.hbase.coprocessor.HBaseSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.hbase.ZkTimestampBlockManager;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import java.io.IOException;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends SpliceBaseMasterObserver {

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        spliceStartAction(ctx);
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        splicePreStopMasterAction(ctx);
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
            splicePreCreateTableAction(ctx, (HTableDescriptor) desc, (HRegionInfo[]) regions);
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        splicePostStartMasterAction(ctx);
    }
}
