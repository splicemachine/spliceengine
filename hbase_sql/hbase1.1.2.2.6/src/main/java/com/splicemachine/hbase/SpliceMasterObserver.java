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

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;

import java.io.IOException;
import java.util.List;
import java.util.Set;

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
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
            splicePreCreateTableAction(ctx,  desc, regions);
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
        splicePostStartMasterAction(ctx);
    }

        public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex)
            throws IOException {
    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                        List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
                                        String regex) throws IOException {
    }

    @Override
    public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 List<HTableDescriptor> descriptors, String regex) throws IOException {
    }


    @Override
    public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  List<HTableDescriptor> descriptors, String regex) throws IOException {
    }
    @Override
    public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    boolean b) throws IOException {
        return b;
    }

    @Override
    public void postListProcedures(
            ObserverContext<MasterCoprocessorEnvironment> ctx,
            List<ProcedureInfo> procInfoList) throws IOException {
    }

    @Override
    public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                        Set<HostAndPort> servers, String targetGroup) throws IOException{}

    @Override
    public void postMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                         Set<HostAndPort> servers, String targetGroup) throws IOException{}
}
