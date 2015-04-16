package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.stats.StatisticsJob;
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
import com.splicemachine.utils.IntArrays;
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


    @SuppressWarnings("UnusedDeclaration")
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
                    //need to make sure it's not a pk or indexed column
                    ensureNotKeyed(descriptor,td);
                    descriptor.setCollectStatistics(false);
                    LanguageConnectionContext languageConnection = conn.getLanguageConnection();
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=false where " +
                            "referenceid = ? and columnname = ?");
                    ps.setString(1,td.getUUID().toString());
                    ps.setString(2,columnName);

                    ps.execute();
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName,schema+"."+table);

        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        }
    }


    @SuppressWarnings("UnusedDeclaration")
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
                    DataTypeDescriptor type=descriptor.getType();
                    if(!allowsStatistics(type.getNull().getTypeFormatId()))
                        throw ErrorState.LANG_COLUMN_STATISTICS_NOT_POSSIBLE.newException(columnName,type.getTypeName());
                    descriptor.setCollectStatistics(true);
                    PreparedStatement ps = conn.prepareStatement("update SYS.SYSCOLUMNS set collectstats=true where " +
                                                                 "referenceid = ? and columnnumber = ?");
                    ps.setString(1,td.getUUID().toString());
                    ps.setInt(2,descriptor.getPosition());

                    ps.execute();
                    return;
                }
            }
            throw ErrorState.LANG_COLUMN_NOT_FOUND_IN_TABLE.newException(columnName,schema+"."+table);

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

    @SuppressWarnings("unused")
    public static void COLLECT_SCHEMA_STATISTICS(String schema, boolean staleOnly,ResultSet[] outputResults) throws SQLException{
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        try{
            if(schema ==null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException(); //TODO -sf- change this to proper SCHEMA error?
            schema = schema.toUpperCase();

            LanguageConnectionContext lcc=conn.getLanguageConnection();
            DataDictionary dd = lcc.getDataDictionary();

            SchemaDescriptor sd = getSchemaDescriptor(schema,lcc,dd);
            //get a list of all the TableDescriptors in the schema
            List<TableDescriptor> tds = getAllTableDescriptors(sd,conn);

            ExecRow templateOutputRow = buildOutputTemplateRow();
            List<StatsJob> jobs = new ArrayList<>(tds.size());
            TransactionController transactionExecute = lcc.getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
            for(TableDescriptor td:tds){
                submitStatsCollection(td,txn,templateOutputRow,conn,staleOnly,jobs);
            }

            List<ExecRow> rows = new ArrayList<>(jobs.size());
            for(StatsJob row:jobs){
                rows.add(row.completeJob());
            }

            IteratorNoPutResultSet results = wrapResults(conn,rows);

            outputResults[0] = new EmbedResultSet40(conn,results,false,null,true);

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }catch(ExecutionException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e.getCause()));
        }catch(InterruptedException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

    }

    @SuppressWarnings("UnusedDeclaration")
    public static void COLLECT_TABLE_STATISTICS(String schema,
                                                String table,
                                                boolean staleOnly,
                                                ResultSet[] outputResults) throws SQLException{
        EmbedConnection conn = (EmbedConnection)SpliceAdmin.getDefaultConn();
        try{
            if(schema==null)
                schema = "SPLICE";
            if(table==null)
                throw ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException();

            schema = schema.toUpperCase();
            table =table.toUpperCase();
            TableDescriptor tableDesc = verifyTableExists(conn,schema,table);
            List<StatsJob> collectionFutures = new LinkedList<>();

            ExecRow outputRow=buildOutputTemplateRow();
            TransactionController transactionExecute = conn.getLanguageConnection().getTransactionExecute();
            transactionExecute.elevate("statistics");
            TxnView txn = ((SpliceTransactionManager) transactionExecute).getRawTransaction().getActiveStateTxn();
            submitStatsCollection(tableDesc,txn,outputRow,conn,staleOnly,collectionFutures);

            List<ExecRow> rows = new ArrayList<>(collectionFutures.size());
            for(StatsJob row:collectionFutures){
                rows.add(row.completeJob());
            }

            IteratorNoPutResultSet resultsToWrap=wrapResults(conn,rows);

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

    private static void submitStatsCollection(TableDescriptor table,
                                              TxnView txn,
                                                ExecRow templateOutputRow,
                                                EmbedConnection conn,
                                                boolean staleOnly,
                                                List<StatsJob> outputJobs) throws StandardException, ExecutionException{
        List<ColumnDescriptor> colsToCollect = getCollectedColumns(table);
        if(colsToCollect.size()<=0){
             //There are no columns to collect for this table. Issue a warning, but proceed anyway
            //TODO -sf- make this a warning
        }

        DataValueDescriptor[] dvds = templateOutputRow.getRowArray();
        dvds[0].setValue(table.getSchemaName());

        //get the indices for the table
        IndexLister indexLister = table.getIndexLister();
        IndexRowGenerator[] indexGenerators;
        if(indexLister!=null) {
            indexGenerators = indexLister.getDistinctIndexRowGenerators();
        }else
            indexGenerators = new IndexRowGenerator[]{};

        //submit the base job
        long heapConglomerateId = table.getHeapConglomerateId();
        Collection<HRegionInfo> regionsToCollect = getCollectedRegions(conn,heapConglomerateId,staleOnly);
        int partitionSize = regionsToCollect.size();
        dvds[1].setValue(table.getName());
        dvds[2].setValue(partitionSize);

        JobScheduler<CoprocessorJob> jobScheduler = SpliceDriver.driver().getJobScheduler();
        StatisticsJob job = getBaseTableStatisticsJob(conn,table, colsToCollect, regionsToCollect, txn);
        JobFuture jobFuture = jobScheduler.submit(job);
        outputJobs.add(new StatsJob(templateOutputRow.getClone(),jobFuture,job));

        //submit all index collection jobs
        if(indexGenerators.length>0) {
            //we know this is safe because we've actually already checked for it
            @SuppressWarnings("ConstantConditions") long[] indexConglomIds = indexLister.getDistinctIndexConglomerateNumbers();
            String[] distinctIndexNames = indexLister.getDistinctIndexNames();
            for (int i = 0; i < indexGenerators.length; i++) {
                IndexRowGenerator irg = indexGenerators[i];
                long indexConglomId = indexConglomIds[i];
                Collection<HRegionInfo> indexRegions = getCollectedRegions(conn, indexConglomId, staleOnly);
                StatisticsJob indexJob = getIndexJob(irg, indexConglomId,
                        heapConglomerateId,table, indexRegions,colsToCollect,txn);
                JobFuture future = jobScheduler.submit(indexJob);
                templateOutputRow.getColumn(2).setValue(distinctIndexNames[i]);
                outputJobs.add(new StatsJob(templateOutputRow.getClone(),future,indexJob));
            }
        }
    }


    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn,List<ExecRow> rows) throws StandardException{
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows,COLLECTED_STATS_OUTPUT_COLUMNS,lastActivation);
        resultsToWrap.openCore();
        return resultsToWrap;
    }

    private static ExecRow buildOutputTemplateRow() throws StandardException{
        ExecRow outputRow =  new ValueRow(COLLECTED_STATS_OUTPUT_COLUMNS.length);
        DataValueDescriptor[] dvds = new DataValueDescriptor[COLLECTED_STATS_OUTPUT_COLUMNS.length];
        for(int i=0;i<dvds.length;i++){
            dvds[i] = COLLECTED_STATS_OUTPUT_COLUMNS[i].getType().getNull();
        }
        outputRow.setRowArray(dvds);
        return outputRow;
    }

    private static List<TableDescriptor> getAllTableDescriptors(SchemaDescriptor sd,EmbedConnection conn) throws SQLException{
        try(Statement statement = conn.createStatement()){
            String sql="select tableid from sys.systables t where t.schemaid = '"+sd.getUUID().toString()+"'";
            try(ResultSet resultSet=statement.executeQuery(sql)){
                DataDictionary dd = conn.getLanguageConnection().getDataDictionary();
                UUIDFactory uuidFactory=dd.getUUIDFactory();
                List<TableDescriptor> tds = new LinkedList<>();
                while(resultSet.next()){
                    com.splicemachine.db.catalog.UUID tableId=uuidFactory.recreateUUID(resultSet.getString(1));
                    TableDescriptor tableDescriptor=dd.getTableDescriptor(tableId);
                    /*
                     * We need to filter out views from the TableDescriptor list. Views
                     * are special cases where the number of conglomerate descriptors is 0. We
                     * don't collect statistics for those views
                     */
                    if(tableDescriptor.getConglomerateDescriptorList().size()>0){
                        tds.add(tableDescriptor);
                    }
                }
                return tds;
            }
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }

    private static StatisticsJob getIndexJob(IndexRowGenerator irg,
                                             long indexConglomerateId,
                                             long heapConglomerateId,
                                             TableDescriptor tableDesc,
                                             Collection<HRegionInfo> indexRegions,
                                             List<ColumnDescriptor> columnsToCollect,
                                             TxnView baseTxn) throws StandardException{
        ExecRow indexRow = irg.getIndexRowTemplate();
        int[] baseColumnPositions = irg.baseColumnPositions();
        int keyCols = indexRow.nColumns();
        if(irg.isUnique())keyCols--;

        int[] fieldLengths = new int[indexRow.nColumns()];
        int[] keyBasePositionMap = new int[indexRow.nColumns()];

        int[] keyTypes = new int[keyCols];
        int[] keyEncodingOrder = new int[keyCols];
        boolean[] keySortOrder = new boolean[keyCols];
        System.arraycopy(irg.isAscending(),0,keySortOrder,0,irg.isAscending().length);
        int[] keyDecodingMap =IntArrays.count(keyCols);
        int nextColPos = 1;
        for(int keyColumn:baseColumnPositions){
            for(ColumnDescriptor cd:columnsToCollect){
                int colPos = cd.getPosition();
                if(colPos==keyColumn){
                    /*
                     * We have the column information for this position in the row, so fill
                     * the value
                     */
                    DataTypeDescriptor type=cd.getType();
                    DataValueDescriptor val=type.getNull();
                    indexRow.setColumn(nextColPos,val);
                    keyTypes[nextColPos-1] = val.getTypeFormatId();
                    keyEncodingOrder[nextColPos-1] = colPos-1;
                    fieldLengths[nextColPos] = type.getMaximumWidth();
                    keyBasePositionMap[nextColPos-1] = colPos;
                    nextColPos++;
                    break;
                }
            }
        }
//        int nextColPos = 1;
//        for(ColumnDescriptor cd:columnsToCollect){
//            int colPos = cd.getPosition();
//            for(int keyColumn:baseColumnPositions){
//                if(keyColumn==colPos){
//                    //we have an index column! hooray! put it in the next position in the row
//                    DataTypeDescriptor type=cd.getType();
//                    DataValueDescriptor val=type.getNull();
//                    indexRow.setColumn(nextColPos,val);
//                    keyTypes[nextColPos-1] = val.getTypeFormatId();
//                    keyEncodingOrder[nextColPos-1] = colPos-1;
//                    keyBasePositionMap[nextColPos-1] = colPos;
//                    fieldLengths[nextColPos] = type.getMaximumWidth();
//                    nextColPos++;
//                    break;
//                }
//            }
//        }
        HBaseRowLocation value=new HBaseRowLocation();
        indexRow.setColumn(indexRow.nColumns(),value);

        int[] rowDecodingMap;
        if(!irg.isUnique()){
            keySortOrder[indexRow.nColumns()-1] = true;
            keyTypes[indexRow.nColumns()-1]=value.getTypeFormatId();
            rowDecodingMap=new int[0];
        }else{
            keyBasePositionMap[nextColPos-1] = -1;
            rowDecodingMap= new int[]{indexRow.nColumns()-1};
        }
        FormatableBitSet collectedKeyColumns = new FormatableBitSet(baseColumnPositions.length);
        for(int i=0;i<keyEncodingOrder.length;i++){
            collectedKeyColumns.grow(i+1);
            collectedKeyColumns.set(i);
        }

        String jobId = "Statistics-"+SpliceDriver.driver().getUUIDGenerator().nextUUID();
        StatisticsTask baseTask = new StatisticsTask(jobId,indexRow,
                keyBasePositionMap,
                rowDecodingMap,
                keyDecodingMap,
                keyEncodingOrder,
                keySortOrder,
                keyTypes,
                fieldLengths,
                collectedKeyColumns,
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
            columnPositionMap[outputCol] = descriptor.getPosition();
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
                columnPositionMap,
                rowDecodingMap,
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
        SchemaDescriptor schemaDescriptor=getSchemaDescriptor(schema,lcc,dd);
        TableDescriptor tableDescriptor = dd.getTableDescriptor(table,schemaDescriptor,lcc.getTransactionExecute());
        if(tableDescriptor==null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema+"."+table);

        return tableDescriptor;
    }

    private static SchemaDescriptor getSchemaDescriptor(String schema,
                                                        LanguageConnectionContext lcc,
                                                        DataDictionary dd) throws StandardException{
        SchemaDescriptor schemaDescriptor = dd.getSchemaDescriptor(schema,lcc.getTransactionExecute(),true);
        if(schemaDescriptor==null)
            throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schema);
        return schemaDescriptor;
    }

    private static final Comparator<ColumnDescriptor> order = new Comparator<ColumnDescriptor>(){
        @Override
        public int compare(ColumnDescriptor o1,ColumnDescriptor o2){
            return o1.getPosition()-o2.getPosition();
        }
    };
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
        Collections.sort(toCollect,order); //sort the columns into adjacent position order
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

    private static void ensureNotKeyed(ColumnDescriptor descriptor,TableDescriptor td) throws StandardException{
        ConglomerateDescriptor heapConglom=td.getConglomerateDescriptor(td.getHeapConglomerateId());
        IndexRowGenerator pkDescriptor=heapConglom.getIndexDescriptor();
        if(pkDescriptor!=null && pkDescriptor.getIndexDescriptor()!=null){
            for(int pkCol : pkDescriptor.baseColumnPositions()){
                if(pkCol==descriptor.getPosition()){
                    throw ErrorState.LANG_DISABLE_STATS_FOR_KEYED_COLUMN.newException(descriptor.getColumnName());
                }
            }
        }
        IndexLister indexLister=td.getIndexLister();
        if(indexLister!=null){
            for(IndexRowGenerator irg:indexLister.getIndexRowGenerators()){
                if(irg.getIndexDescriptor()==null) continue;
                for(int col:irg.baseColumnPositions()){
                    if(col==descriptor.getPosition())
                        throw ErrorState.LANG_DISABLE_STATS_FOR_KEYED_COLUMN.newException(descriptor.getColumnName());
                }
            }
        }
    }

    private static boolean allowsStatistics(int typeFormatId){
        switch(typeFormatId){
            case StoredFormatIds.SQL_BOOLEAN_ID:
            case StoredFormatIds.SQL_TINYINT_ID:
            case StoredFormatIds.SQL_SMALLINT_ID:
            case StoredFormatIds.SQL_INTEGER_ID:
            case StoredFormatIds.SQL_LONGINT_ID:
            case StoredFormatIds.SQL_REAL_ID:
            case StoredFormatIds.SQL_DOUBLE_ID:
            case StoredFormatIds.SQL_DECIMAL_ID:
            case StoredFormatIds.SQL_CHAR_ID:
            case StoredFormatIds.SQL_DATE_ID:
            case StoredFormatIds.SQL_TIME_ID:
            case StoredFormatIds.SQL_TIMESTAMP_ID:
            case StoredFormatIds.SQL_VARCHAR_ID:
            case StoredFormatIds.SQL_LONGVARCHAR_ID:
                return true;
            default:
                return false;
        }
    }

    private static class StatsJob{
        private ExecRow outputRow;
        private JobFuture jobFuture;
        private StatisticsJob job;

        public StatsJob(ExecRow outputRow,JobFuture jobFuture,StatisticsJob job){
            this.outputRow=outputRow;
            this.jobFuture=jobFuture;
            this.job=job;
        }

        private ExecRow completeJob() throws ExecutionException, InterruptedException, StandardException {
            jobFuture.completeAll(null);
            jobFuture.cleanup(); //cleanup the task stuff

            //invalidate the cached statistics, since we have new stats to use
            job.invalidateStatisticsCache();

            List<TaskStats> taskStats = jobFuture.getJobStats().getTaskStats();
            long totalRows = 0l;
            for( TaskStats stats:taskStats){
                totalRows+=stats.getTotalRowsProcessed();
            }

            outputRow.getColumn(4).setValue(taskStats.size());
            outputRow.getColumn(5).setValue(totalRows);

            return outputRow;
        }
    }
}
