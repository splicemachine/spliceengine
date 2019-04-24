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

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends SpliceBaseMasterObserver implements MasterCoprocessor {
    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        spliceStartAction(ctx);
    }

    @Override
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        splicePreStopMasterAction(ctx);
    }

    @Override
    public void preCreateTableAction(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "SpliceMasterObserver.preCreateTable()");

        splicePreCreateTableAction(ctx, desc.getTableName(), null);
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        splicePostStartMasterAction(ctx);
    }


    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }
}
