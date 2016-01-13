package com.splicemachine.derby.stream.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.stats.SimpleOverheadManagedPartitionStatistics;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.StatisticsAdmin;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class StatisticsScanner<Data> extends SITableScanner<Data>{
    private boolean initialized;
    private List<ExecRow> rows;
    private int[] columnPositionMap;
    private long conglomId;
    private String regionId;
    private StatisticsCollector collector;
    SimpleOverheadManagedPartitionStatistics statistics;

    public StatisticsScanner(long conglomId,
                             final SDataLib dataLib,
                             DataScanner scanner,
                             final TransactionalRegion region,
                             final ExecRow template,
                             DataScan scan,
                             final int[] rowDecodingMap,
                             final TxnView txn,
                             int[] keyColumnEncodingOrder,
                             boolean[] keyColumnSortOrder,
                             int[] keyColumnTypes,
                             int[] keyDecodingMap,
                             FormatableBitSet accessedPks,
                             boolean reuseRowLocation,
                             String indexName,
                             final String tableVersion,
                             SIFilterFactory filterFactory,
                             int[] fieldLengths,
                             int[] columnPositionMap){
        super(scanner,region,template,scan,rowDecodingMap,txn,keyColumnEncodingOrder,
                keyColumnSortOrder,keyColumnTypes,keyDecodingMap,accessedPks,reuseRowLocation,indexName,
                tableVersion,filterFactory);
        this.columnPositionMap=columnPositionMap;
        this.conglomId = conglomId;
//        MeasuredRegionScanner regionScanner = getRegionScanner();
//        HRegionInfo r = regionScanner.getRegionInfo();
//        conglomId = r.getTable().getQualifierAsString();
        regionId = region.getTableName();
        collector=new StatisticsCollector(txn,template,columnPositionMap,fieldLengths,this);
    }

    @Override
    public ExecRow next() throws StandardException, IOException{
        if(!initialized){
            initialize();
        }
        if(rows.size()==0)
            return null;
        currentRowLocation=new HBaseRowLocation(EngineDriver.driver().newUUIDGenerator(1).nextBytes());
        return rows.remove(0);
    }

    private void initialize() throws StandardException, IOException{
        try{
            ExecRow next;
            while((next=super.next())!=null){
                collector.collect(next);
            }
            statistics=collector.getStatistics();
            List<ColumnStatistics> columnStatisticsList=statistics.columnStatistics();
            rows=new ArrayList<>(columnStatisticsList.size()+1);
            for(int i=0;i<columnStatisticsList.size();i++){
                if(columnStatisticsList.get(i)==null)
                    continue;
                rows.add(StatisticsAdmin.generateRowFromStats(conglomId,regionId,columnPositionMap[i],columnStatisticsList.get(i)));
            }
            rows.add(StatisticsAdmin.generateRowFromStats(conglomId,regionId,statistics));
            initialized=true;
        }catch(ExecutionException e){
            throw StandardException.plainWrapException(e);
        }
    }

    @Override
    public void close() throws StandardException, IOException{
        super.close();
    }

}
