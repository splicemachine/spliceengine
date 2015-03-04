package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Statistics collector entity. Designed to be shared by the two types of tasks (maintenance
 * and triggered collections).
 *
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsCollector {
    private final TxnView txn;
    private final ExecRow template;
    private final Scan partitionScan;
    private final int[] rowDecodingMap;
    private final int[] keyColumnEncodingOrder;
    private final boolean[] keyColumnSortOrder;
    private final int[] keyColumnTypes;
    private final int[] keyDecodingMap;
    private final FormatableBitSet collectedKeyColumns;
    private final String tableVersion;
    private final TransactionalRegion txnRegion;
    private final MeasuredRegionScanner scanner;

    public StatisticsCollector(TxnView txn,
                               ExecRow template,
                               Scan partitionScan,
                               int[] rowDecodingMap,
                               int[] keyColumnEncodingOrder,
                               boolean[] keyColumnSortOrder,
                               int[] keyColumnTypes,
                               int[] keyDecodingMap,
                               FormatableBitSet collectedKeyColumns,
                               String tableVersion,
                               TransactionalRegion txnRegion,
                               MeasuredRegionScanner scanner
                               ) {
        this.txn = txn;
        this.template = template;
        this.partitionScan = partitionScan;
        this.rowDecodingMap = rowDecodingMap;
        this.keyColumnEncodingOrder = keyColumnEncodingOrder;
        this.keyColumnSortOrder = keyColumnSortOrder;
        this.keyColumnTypes = keyColumnTypes;
        this.keyDecodingMap = keyDecodingMap;
        this.collectedKeyColumns = collectedKeyColumns;
        this.tableVersion = tableVersion;
        this.txnRegion = txnRegion;
        this.scanner = scanner;
    }

    @SuppressWarnings("unchecked")
    public PartitionStatistics collect() throws ExecutionException {
        List<ColumnStatistics> columnStats;
        try(SITableScanner scanner = getTableScanner()){
            ColumnStatsCollector<DataValueDescriptor>[] dvdCollectors = getCollectors();
            int[] fieldLengths = new int[dvdCollectors.length];
            ExecRow row;
            while((row = scanner.next(null))!=null){
                scanner.recordFieldLengths(fieldLengths); //get the size of each column
                DataValueDescriptor[] dvds = row.getRowArray();
                for (int i = 0; i < dvds.length; i++) {
                    DataValueDescriptor dvd = dvds[i];
                    dvdCollectors[i].update(dvd);
                    dvdCollectors[i].updateSize(fieldLengths[i]);
                }
            }

            columnStats = new ArrayList<>(dvdCollectors.length);
            for(int i=0;i<dvdCollectors.length;i++){
                columnStats.add(dvdCollectors[i].build());
            }

            TimeView readTime = scanner.getTime();
            long byteCount = scanner.getBytesVisited();
            long rowCount = scanner.getRowsVisited();

            String tableId = txnRegion.getTableName();
            String regionId = txnRegion.getRegionName();
            return new SimplePartitionStatistics(tableId,regionId,
                    rowCount,
                    byteCount,
                    0l, //TODO -sf- get Query count for this region
                    readTime.getWallClockTime(),
                    rowCount>0?readTime.getWallClockTime():0l,
                    columnStats);
        } catch (StandardException | IOException e) {
            throw new ExecutionException(e); //should only be IOExceptions
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    @SuppressWarnings("unchecked")
    private ColumnStatsCollector<DataValueDescriptor>[] getCollectors() {
        int cardinalityPrecision = StatsConstants.cardinalityPrecision;
        int topKSize = StatsConstants.topKSize;
        DataValueDescriptor[] dvds = template.getRowArray();
        ColumnStatsCollector<DataValueDescriptor> [] collectors = new ColumnStatsCollector[dvds.length];
        for(int i=0;i<dvds.length;i++){
            collectors[i] = DvdStatsCollector.newCollector(dvds[i].getTypeFormatId(),cardinalityPrecision,topKSize);
        }
        return collectors;
    }

    private SITableScanner getTableScanner() {
        return new TableScannerBuilder()
                .transaction(txn)
                .template(template)
                .metricFactory(Metrics.basicMetricFactory()) //record latency timings
                .scan(partitionScan)
                .rowDecodingMap(rowDecodingMap)
                .keyColumnEncodingOrder(keyColumnEncodingOrder)
                .keyColumnSortOrder(keyColumnSortOrder)
                .keyColumnTypes(keyColumnTypes)
                .keyDecodingMap(keyDecodingMap)
                .accessedKeyColumns(collectedKeyColumns)
                .tableVersion(tableVersion)
                .region(txnRegion)
                .scanner(scanner)
                .build();
    }
}
