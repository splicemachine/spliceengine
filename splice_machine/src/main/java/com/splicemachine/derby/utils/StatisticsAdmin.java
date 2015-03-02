package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.stats.StatisticsJob;
import com.splicemachine.derby.impl.stats.StatisticsTask;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.job.JobFuture;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsAdmin {

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


            Collection<HRegionInfo> regionsToCollect = getCollectedRegions(conn,tableDesc.getHeapConglomerateId(), staleOnly);
            StatisticsJob job = getStatisticsJob(conn,tableDesc, colsToCollect, regionsToCollect);
            JobFuture jobFuture = SpliceDriver.driver().getJobScheduler().submit(job);
            jobFuture.completeAll(null);

            List<TaskStats> taskStats = jobFuture.getJobStats().getTaskStats();
            long totalRows = 0l;
            for( TaskStats stats:taskStats){
                totalRows+=stats.getTotalRowsProcessed();
            }

            ExecRow outputRow =  new ValueRow(COLLECTED_STATS_OUTPUT_COLUMNS.length);
            DataValueDescriptor[] dvds = new DataValueDescriptor[COLLECTED_STATS_OUTPUT_COLUMNS.length];
            for(int i=0;i<dvds.length;i++){
                dvds[i] = COLLECTED_STATS_OUTPUT_COLUMNS[i].getType().getNull();
            }
            outputRow.setRowArray(dvds);
            dvds[0].setValue(schema);
            dvds[1].setValue(table);
            dvds[2].setValue(regionsToCollect.size());
            dvds[3].setValue(taskStats.size());
            dvds[4].setValue(totalRows);

            Activation lastActivation = conn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(Arrays.asList(outputRow),COLLECTED_STATS_OUTPUT_COLUMNS,lastActivation);
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

    private static StatisticsJob getStatisticsJob(EmbedConnection conn,
                                                  TableDescriptor tableDesc,
                                                  List<ColumnDescriptor> colsToCollect,
                                                  Collection<HRegionInfo> regionsToCollect) throws StandardException {
        ExecRow row = new ValueRow(colsToCollect.size());

        TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
        SpliceConglomerate conglomerate = (SpliceConglomerate)((SpliceTransactionManager) transactionExecute).findConglomerate(tableDesc.getHeapConglomerateId());
        boolean[] keyColumnSortOrder = conglomerate.getAscDescInfo();
        int[] keyColumnEncodingOrder = conglomerate.getColumnOrdering();

        int[] rowDecodingMap = colsToCollect.size()>0?new int[tableDesc.getNumberOfColumns()]: new int[]{};
        int[] keyDecodingMap = new int[keyColumnEncodingOrder.length];
        FormatableBitSet collectedKeyColumns = new FormatableBitSet(keyColumnEncodingOrder.length);
        int[] formatIds = conglomerate.getFormat_ids();
        int[] keyColumnTypes = new int[keyColumnEncodingOrder.length];
        int outputPos = 0;
        for(ColumnDescriptor descriptor: colsToCollect){
            int columnPos = descriptor.getPosition()-1;
            boolean isKey = false;
            for(int i=0;i<keyColumnEncodingOrder.length;i++){
                int col = keyColumnEncodingOrder[i];
                if(col==columnPos){
                    keyDecodingMap[i] = outputPos;
                    keyColumnTypes[i] = formatIds[columnPos];
                    collectedKeyColumns.set(i);
                    isKey = true;
                    break;
                }
            }
            if(!isKey){
                rowDecodingMap[columnPos] = outputPos;
            }
            row.setColumn(outputPos+1,descriptor.getType().getNull());
            outputPos++;
        }

        String jobId = "Statistics-"+SpliceDriver.driver().getUUIDGenerator().nextUUID();
        String tableVersion = tableDesc.getVersion();
        StatisticsTask baseTask = new StatisticsTask(jobId,row,
                rowDecodingMap,
                keyDecodingMap,
                keyColumnEncodingOrder,
                keyColumnSortOrder,
                keyColumnTypes,
                collectedKeyColumns,tableVersion);

        HTableInterface table = SpliceAccessManager.getHTable(tableDesc.getHeapConglomerateId());
        //elevate the parent transaction with both tables
        transactionExecute.elevate("statistics");
        TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
        List<Pair<byte[],byte[]>> regionBounds = new ArrayList<>(regionsToCollect.size());
        for(HRegionInfo info:regionsToCollect){
            regionBounds.add(Pair.newPair(info.getStartKey(),info.getEndKey()));
        }

        return new StatisticsJob(regionBounds,table,baseTask,txn);
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

    private static List<ColumnDescriptor> getCollectedColumns(TableDescriptor td) {
        ColumnDescriptorList columnDescriptorList = td.getColumnDescriptorList();
        List<ColumnDescriptor> toCollect = new ArrayList<>(columnDescriptorList.size());
        for(ColumnDescriptor columnDescriptor:columnDescriptorList){
            if(columnDescriptor.collectStatistics())
                toCollect.add(columnDescriptor);
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
