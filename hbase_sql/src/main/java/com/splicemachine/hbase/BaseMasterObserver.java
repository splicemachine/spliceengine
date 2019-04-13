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

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Created by jyuan on 4/11/19.
 */
public abstract class BaseMasterObserver implements MasterObserver, Coprocessor{
    
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    }

    
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    }

    
    public void preCreateTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx,
            HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    }

    
    public void postCreateTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx,
            HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    }

    
    public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {
    }

    
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
    }

    
    public void preDeleteTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException{
    }

    
    public void postDeleteTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName) throws IOException {
    }

    
    public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  TableName tableName) throws IOException {
    }

    
    public void preTruncateTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void postTruncateTableHandler(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName, HTableDescriptor htd) throws IOException {
    }

    
    public void postModifyTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HTableDescriptor htd) throws IOException {
    }

    
    public void preModifyTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HTableDescriptor htd) throws IOException {
    }

    
    public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName, HTableDescriptor htd) throws IOException {
    }

    
    public void preCreateNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
            throws IOException {
    }

    
    public void postCreateNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
            throws IOException {
    }

    
    public void preDeleteNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
    }

    
    public void postDeleteNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace) throws IOException {
    }

    
    public void preModifyNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
            throws IOException {
    }

    
    public void postModifyNamespace(
            ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
            throws IOException {
    }

    
    public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                          String namespace) throws IOException {
    }

    
    public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           NamespaceDescriptor ns) throws IOException {
    }

    
    public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                            List<NamespaceDescriptor> descriptors) throws IOException {
    }

    
    public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                             List<NamespaceDescriptor> descriptors) throws IOException {
    }

    
    public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName, HColumnDescriptor column) throws IOException {
    }

    
    public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName, HColumnDescriptor column) throws IOException {
    }

    
    public void preAddColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HColumnDescriptor column) throws IOException {
    }

    
    public void postAddColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HColumnDescriptor column) throws IOException {
    }

    
    public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName, HColumnDescriptor descriptor) throws IOException {
    }

    
    public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName, HColumnDescriptor descriptor) throws IOException {
    }

    
    public void preModifyColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HColumnDescriptor descriptor) throws IOException {
    }

    
    public void postModifyColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            HColumnDescriptor descriptor) throws IOException {
    }

    
    public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName, byte[] c) throws IOException {
    }

    
    public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName, byte[] c) throws IOException {
    }

    
    public void preDeleteColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            byte[] c) throws IOException {
    }

    
    public void postDeleteColumnHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName,
            byte[] c) throws IOException {
    }


    
    public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {
    }

    
    public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
    }

    
    public void preEnableTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void postEnableTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
    }

    
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 TableName tableName) throws IOException {
    }

    
    public void preDisableTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void postDisableTableHandler(
            ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
    }

    
    public void preAbortProcedure(
            ObserverContext<MasterCoprocessorEnvironment> ctx,
            final ProcedureExecutor<MasterProcedureEnv> procEnv,
            final long procId) throws IOException {
    }

    
    public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
    public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
//    public void postListProcedures(
//            ObserverContext<MasterCoprocessorEnvironment> ctx,
//            List<ProcedureInfo> procInfoList) throws IOException {
//    }

    
    public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                          HRegionInfo regionInfo) throws IOException {
    }

    
    public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                           HRegionInfo regionInfo) throws IOException {
    }

    
    public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            HRegionInfo regionInfo, boolean force) throws IOException {
    }

    
    public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             HRegionInfo regionInfo, boolean force) throws IOException {
    }

    
    public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 HRegionInfo regionInfo) throws IOException {
    }

    
    public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  HRegionInfo regionInfo) throws IOException {
    }

    
    public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
    public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan> plans)
            throws IOException {
    }

    
//    public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
//                                    boolean b) throws IOException {
//        return b;
//    }

    
    public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  boolean oldValue, boolean newValue) throws IOException {
    }

    
    public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
    public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
            throws IOException {
    }

    
    public void preMasterInitialization(
            ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    }

    
    public void start(CoprocessorEnvironment ctx) throws IOException {
    }

    
    public void stop(CoprocessorEnvironment ctx) throws IOException {
    }

    
    public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
                        HRegionInfo region, ServerName srcServer, ServerName destServer)
            throws IOException {
    }

    
    public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx,
                         HRegionInfo region, ServerName srcServer, ServerName destServer)
            throws IOException {
    }

    
    public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                            final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void postSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                             final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void preListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                final HBaseProtos.SnapshotDescription snapshot) throws IOException {
    }

    
    public void postListSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final HBaseProtos.SnapshotDescription snapshot) throws IOException {
    }

    
    public void preCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void postCloneSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void preRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void postRestoreSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    final HBaseProtos.SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
            throws IOException {
    }

    
    public void preDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final HBaseProtos.SnapshotDescription snapshot) throws IOException {
    }

    
    public void postDeleteSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   final HBaseProtos.SnapshotDescription snapshot) throws IOException {
    }

    
    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                       List<TableName> tableNamesList, List<HTableDescriptor> descriptors)
            throws IOException {
    }

    
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                        List<HTableDescriptor> descriptors) throws IOException {
    }


    
//    public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
//                                       List<TableName> tableNamesList, List<HTableDescriptor> descriptors, String regex)
//            throws IOException {
//    }
//
//
//    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
//                                        List<TableName> tableNamesList, List<HTableDescriptor> descriptors,
//                                        String regex) throws IOException {
//    }
//
//
//    public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
//                                 List<HTableDescriptor> descriptors, String regex) throws IOException {
//    }
//
//
//    public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
//                                  List<HTableDescriptor> descriptors, String regex) throws IOException {
//    }

    
    public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {
    }

    
    public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {
    }

    
    public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                final String userName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final String userName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                final String userName, final TableName tableName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final String userName, final TableName tableName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void preSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                final String userName, final String namespace, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void postSetUserQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final String userName, final String namespace, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void preSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 final TableName tableName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void postSetTableQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  final TableName tableName, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void preSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     final String namespace, final QuotaProtos.Quotas quotas) throws IOException {
    }

    
    public void postSetNamespaceQuota(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      final String namespace, final QuotaProtos.Quotas quotas) throws IOException {
    }

    public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName>
            tables, String targetGroup) throws IOException {
    }

    
    public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               Set<TableName> tables, String targetGroup) throws IOException {
    }

    
    public void preAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
            throws IOException {
    }

    
    public void postAddRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
            throws IOException {
    }

    
    public void preRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
            throws IOException {

    }

    
    public void postRemoveRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
            throws IOException {
    }

    
    public void preBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName)
            throws IOException {
    }

    
    public void postBalanceRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   String groupName, boolean balancerRan) throws IOException {
    }
}
