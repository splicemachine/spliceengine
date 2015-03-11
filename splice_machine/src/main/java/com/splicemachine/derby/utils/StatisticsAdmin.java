package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.stats.StatisticsJob;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.stats.StatisticsTask;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobScheduler;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.execute.IteratorNoPutResultSet;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsAdmin {


    public static void DISABLE_COLUMN_STATISTICS(String schema,
                                                String table,
                                                String columnName) throws SQLException{
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn,schema,table);
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for(ColumnDescriptor descriptor: columnDescriptorList){
                if(descriptor.getColumnName().equalsIgnoreCase(columnName)){
                    descriptor.setCollectStatistics(true);
                    LanguageConnectionContext languageConnection = conn.getLanguageConnection();
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=false where " +
                            "referenceid = ? and columnname = ?");
                    ps.setString(1,td.getUUID().toString());
                    ps.setString(2,columnName);

                    ps.execute();
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(schema+"."+table,columnName);

        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    public static void ENABLE_COLUMN_STATISTICS(String schema,
                                                String table,
                                                String columnName) throws SQLException{
        if(columnName==null)
            throw PublicAPI.wrapStandardException(ErrorState.LANG_COLUMN_ID.newException());
        columnName = columnName.toUpperCase();
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        try {
            TableDescriptor td = verifyTableExists(conn,schema,table);
            //verify that that column exists
            ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
            for(ColumnDescriptor descriptor: columnDescriptorList){
                if(descriptor.getColumnName().equalsIgnoreCase(columnName)){
                    descriptor.setCollectStatistics(true);
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=true where " +
                                                                 "referenceid = ? and columnnumber = ?");
                    ps.setString(1,td.getUUID().toString());
                    ps.setInt(2,descriptor.getPosition());

                    ps.execute();
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(schema+"."+table,columnName);

        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static final ResultColumnDescriptor[] COLLECTED_STATS_OUTPUT_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("schemaName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("tableName", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("regionsCollected",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("tasksExecuted",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("rowsCollected",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };
    public static void COLLECT_TABLE_STATISTICS(String schema,
                                                String table,
                                                boolean staleOnly,
                                                ResultSet[] outputResults) throws SQLException{
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        try{
            if(schema==null)
                schema = "SPLICE";
            schema = schema.toUpperCase();
            table = table!=null? table.toUpperCase(): table; //TODO -sf- deal with this situation
            TableDescriptor tableDesc = verifyTableExists(conn,schema,table);

            List<ColumnDescriptor> colsToCollect = getCollectedColumns(tableDesc);
            if(colsToCollect.size()<=0){
                 //There are no columns to collect for this table. Issue a warning, but proceed anyway
                //TODO -sf- make this a warning
            }

            ExecRow outputRow =  new ValueRow(COLLECTED_STATS_OUTPUT_COLUMNS.length);
            DataValueDescriptor[] dvds = new DataValueDescriptor[COLLECTED_STATS_OUTPUT_COLUMNS.length];
            for(int i=0;i<dvds.length;i++){
                dvds[i] = COLLECTED_STATS_OUTPUT_COLUMNS[i].getType().getNull();
            }
            dvds[0].setValue(schema);

            //get the indices for the table
            IndexLister indexLister = tableDesc.getIndexLister();
            IndexRowGenerator[] indexGenerators;
            if(indexLister!=null) {
                indexGenerators = indexLister.getDistinctIndexRowGenerators();
            }else
                indexGenerators = new IndexRowGenerator[]{};

            //submit the base job
            long heapConglomerateId = tableDesc.getHeapConglomerateId();
            Collection<HRegionInfo> regionsToCollect = getCollectedRegions(conn, heapConglomerateId, staleOnly);
            int partitionSize = regionsToCollect.size();
            dvds[1].setValue(table);
            dvds[2].setValue(partitionSize);
            outputRow.setRowArray(dvds);

            JobScheduler<CoprocessorJob> jobScheduler = SpliceDriver.driver().getJobScheduler();
            TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
            List<Pair<ExecRow,JobFuture>> collectionFutures = new ArrayList<>(indexGenerators.length+1);
            StatisticsJob job = getBaseTableStatisticsJob(conn, tableDesc, colsToCollect, regionsToCollect, txn);
            JobFuture jobFuture = jobScheduler.submit(job);
            collectionFutures.add(new Pair<>(outputRow.getClone(), jobFuture));

            //submit all index collection jobs
            if(indexGenerators.length>0) {
                long[] indexConglomIds = indexLister.getDistinctIndexConglomerateNumbers();
                String[] distinctIndexNames = indexLister.getDistinctIndexNames();
                for (int i = 0; i < indexGenerators.length; i++) {
                    IndexRowGenerator irg = indexGenerators[i];
                    long indexConglomId = indexConglomIds[i];
                    Collection<HRegionInfo> indexRegions = getCollectedRegions(conn, indexConglomId, staleOnly);
                    StatisticsJob indexJob = getIndexJob(irg, indexConglomId, heapConglomerateId, tableDesc, indexRegions, txn);
                    JobFuture future = jobScheduler.submit(indexJob);
                    outputRow.getColumn(2).setValue(distinctIndexNames[i]);
                    collectionFutures.add(new Pair<>(outputRow.getClone(),future));
                }
            }

            List<ExecRow> rows = new ArrayList<>(collectionFutures.size());
            for(Pair<ExecRow,JobFuture> row:collectionFutures){
                ExecRow statusRow = row.getFirst();
                completeJob(statusRow,row.getSecond());
                rows.add(statusRow);
            }

            //invalidate statistics stored in the cache, since we know we have new data
            if(StatisticsStorage.isStoreRunning())
                StatisticsStorage.getPartitionStore().invalidateCachedStatistics(heapConglomerateId);

            Activation lastActivation = conn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows,COLLECTED_STATS_OUTPUT_COLUMNS,lastActivation);
            resultsToWrap.openCore();

            outputResults[0] = new EmbedResultSet40(conn,resultsToWrap,false,null,true);

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        } catch (ExecutionException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        } catch (InterruptedException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/


    private static void completeJob(ExecRow outputRow, JobFuture jobFuture) throws ExecutionException, InterruptedException, StandardException {
        jobFuture.completeAll(null);

        List<TaskStats> taskStats = jobFuture.getJobStats().getTaskStats();
        long totalRows = 0l;
        for( TaskStats stats:taskStats){
            totalRows+=stats.getTotalRowsProcessed();
        }

        outputRow.getColumn(4).setValue(taskStats.size());
        outputRow.getColumn(5).setValue(totalRows);
    }

    private static StatisticsJob getIndexJob(IndexRowGenerator irg,
                                             long indexConglomerateId,
                                             long heapConglomerateId,
                                             TableDescriptor tableDesc,
                                             Collection<HRegionInfo> indexRegions,
                                             TxnView baseTxn) {
        ExecRow indexRow = new ValueRow(1);
        indexRow.getRowArray()[0] = new HBaseRowLocation();
        int[] rowDecodingMap = new int[irg.numberOfOrderedColumns()+1];
        Arrays.fill(rowDecodingMap,-1); //just to be safe
        rowDecodingMap[rowDecodingMap.length-1] = 0;

        String jobId = "Statistics-"+SpliceDriver.driver().getUUIDGenerator().nextUUID();
        StatisticsTask baseTask = new StatisticsTask(jobId,indexRow,
                rowDecodingMap,
                rowDecodingMap,
                null,
                null,
                null,
                null,
                null,
                null,
                heapConglomerateId,
                tableDesc.getVersion());

        HTableInterface table = SpliceAccessManager.getHTable(Long.toString(indexConglomerateId).getBytes());
        //elevate the parent transaction with both tables
        List<Pair<byte[],byte[]>> regionBounds = new ArrayList<>(indexRegions.size());
        for(HRegionInfo info:indexRegions){
            regionBounds.add(Pair.newPair(info.getStartKey(),info.getEndKey()));
        }

        return new StatisticsJob(regionBounds,table,baseTask,baseTxn);
    }

    private static StatisticsJob getBaseTableStatisticsJob(EmbedConnection conn,
                                                           TableDescriptor tableDesc,
                                                           List<ColumnDescriptor> colsToCollect,
                                                           Collection<HRegionInfo> regionsToCollect,
                                                           TxnView baseTxn) throws StandardException {
        ExecRow row = new ValueRow(colsToCollect.size());
        BitSet accessedColumns = new BitSet(tableDesc.getNumberOfColumns());
        int outputCol = 0;
        int[] columnPositionMap = new int[tableDesc.getNumberOfColumns()];
        Arrays.fill(columnPositionMap,-1);
        int[] allColumnLengths = new int[tableDesc.getNumberOfColumns()];
        for(ColumnDescriptor descriptor:colsToCollect){
            accessedColumns.set(descriptor.getPosition()-1);
            row.setColumn(outputCol+1,descriptor.getType().getNull());
            columnPositionMap[outputCol] = descriptor.getPosition()-1;
            outputCol++;
            allColumnLengths[descriptor.getPosition()-1] = descriptor.getType().getMaximumWidth();
        }

        int[] rowDecodingMap = new int[accessedColumns.length()];
        int[] fieldLengths = new int[accessedColumns.length()];
        Arrays.fill(rowDecodingMap,-1);
        outputCol = 0;
        for(int i=accessedColumns.nextSetBit(0);i>=0;i = accessedColumns.nextSetBit(i+1)){
            rowDecodingMap[i] = outputCol;
            fieldLengths[outputCol] = allColumnLengths[i];
            outputCol++;
        }
        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate)((SpliceTransactionManager) transactionExecute).findConglomerate(tableDesc.getHeapConglomerateId());
        boolean[] keyColumnSortOrder = conglomerate.getAscDescInfo();
        int[] keyColumnEncodingOrder = conglomerate.getColumnOrdering();
        int[] formatIds = conglomerate.getFormat_ids();
        int[] keyColumnTypes= null;
        int[] keyDecodingMap = null;
        FormatableBitSet collectedKeyColumns = null;
        if(keyColumnEncodingOrder!=null){
            keyColumnTypes = new int[keyColumnEncodingOrder.length];
            keyDecodingMap = new int[keyColumnEncodingOrder.length];
            Arrays.fill(keyDecodingMap,-1);
            collectedKeyColumns = new FormatableBitSet(tableDesc.getNumberOfColumns());
            for(int i=0;i<keyColumnEncodingOrder.length;i++){
                int keyColumn = keyColumnEncodingOrder[i];
                keyColumnTypes[i] = formatIds[keyColumn];
                if(accessedColumns.get(keyColumn)){
                    collectedKeyColumns.set(i);
                    keyDecodingMap[i] = rowDecodingMap[keyColumn];
                    rowDecodingMap[keyColumn] = -1;
                }
            }
        }

        String jobId = "Statistics-"+SpliceDriver.driver().getUUIDGenerator().nextUUID();
        String tableVersion = tableDesc.getVersion();
        StatisticsTask baseTask = new StatisticsTask(jobId,row,
                rowDecodingMap,
                columnPositionMap,
                keyDecodingMap,
                keyColumnEncodingOrder,
                keyColumnSortOrder,
                keyColumnTypes,
                fieldLengths,
                collectedKeyColumns,
                tableVersion);

        HTableInterface table = SpliceAccessManager.getHTable(tableDesc.getHeapConglomerateId());
        //elevate the parent transaction with both tables
        List<Pair<byte[],byte[]>> regionBounds = new ArrayList<>(regionsToCollect.size());
        for(HRegionInfo info:regionsToCollect){
            regionBounds.add(Pair.newPair(info.getStartKey(),info.getEndKey()));
        }

        return new StatisticsJob(regionBounds,table,baseTask,baseTxn);
    }

    private static TableDescriptor verifyTableExists(Connection conn,String schema, String table) throws SQLException, StandardException {
        LanguageConnectionContext lcc = ((EmbedConnection) conn).getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        SchemaDescriptor schemaDescriptor = dd.getSchemaDescriptor(schema,lcc.getTransactionExecute(),true);
        if(schemaDescriptor==null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema);
        TableDescriptor tableDescriptor = dd.getTableDescriptor(table, schemaDescriptor, lcc.getTransactionExecute());
        if(tableDescriptor==null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema+"."+table);

        return tableDescriptor;
    }

    private static List<ColumnDescriptor> getCollectedColumns(TableDescriptor td) throws StandardException {
        ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
        List<ColumnDescriptor> toCollect = new ArrayList<>(columnDescriptorList.size());
        /*
         * Get all the enabled statistics columns
         */
        for(ColumnDescriptor columnDescriptor:columnDescriptorList){
            if(columnDescriptor.collectStatistics())
                toCollect.add(columnDescriptor);
        }
        /*
         * Add in any disabled key columns.
         *
         * We want to collect for all key columns always, because they are very important when
         * comparing index columns. By default, we turn them on when possible, but even if they are disabled
         * for some reason, we should still collect them. Of course, we should also not be able to disable
         * keyed columns, but that's a to-do for now.
         */
        IndexLister indexLister = td.getIndexLister();
        if(indexLister!=null){
            IndexRowGenerator[] distinctIndexRowGenerators = indexLister.getDistinctIndexRowGenerators();
            for(IndexRowGenerator irg:distinctIndexRowGenerators){
                int[] keyColumns = irg.getIndexDescriptor().baseColumnPositions();
                for(int keyColumn:keyColumns){
                   for(ColumnDescriptor cd:columnDescriptorList){
                       if(cd.getPosition()==keyColumn){
                           if(!toCollect.contains(cd)) {
                               toCollect.add(cd);
                           }
                           break;
                       }
                   }
                }
            }
        }
        return toCollect;
    }

    private static Collection<HRegionInfo> getCollectedRegions(Connection conn,long heapConglomerateId, boolean staleOnly) throws StandardException {
        //TODO -sf- adjust the regions if staleOnly == true
        return getAllRegions(heapConglomerateId);
    }

    private static Collection<HRegionInfo> getAllRegions(long heapConglomerateId) throws StandardException {
        RegionCache instance = HBaseRegionCache.getInstance();
        byte[] tableName = Long.toString(heapConglomerateId).getBytes();
        instance.invalidate(tableName); //invalidate to force the most up-to-date listing
        try {
            SortedSet<Pair<HRegionInfo, ServerName>> regions = instance.getRegions(tableName);
            SortedSet<HRegionInfo> toCollect = new TreeSet<>();
            for(Pair<HRegionInfo,ServerName> region:regions){
                //fetch the latest staleness data for that
                toCollect.add(region.getFirst());
            }
            return toCollect;
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        }
    }
}
