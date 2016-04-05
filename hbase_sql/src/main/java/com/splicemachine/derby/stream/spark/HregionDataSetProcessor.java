package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.IndexTableScannerBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.IndexScanSetBuilder;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jleach on 4/13/15.
 */
public class HregionDataSetProcessor extends ControlDataSetProcessor {
    private static final Logger LOG = Logger.getLogger(HregionDataSetProcessor.class);


    public HregionDataSetProcessor(TxnSupplier txnSupplier,
                                   Transactor transactory,
                                   TxnOperationFactory txnOperationFactory){
        super(txnSupplier, transactory, txnOperationFactory);
    }

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(final Op spliceOperation, final String tableName) throws StandardException{
        return new TableScannerBuilder<V>(){
            @Override
            public DataSet<V> buildDataSet() throws StandardException{
                try{
                    TableName tableInfo = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(tableName);
                    HTable htable = new HTable(HBaseConfiguration.create(HConfiguration.unwrapDelegate()), tableInfo);

                    SIDriver driver = SIDriver.driver();

                    HBaseConnectionFactory instance = HBaseConnectionFactory.getInstance(driver.getConfiguration());
                    Clock clock = driver.getClock();
                    Connection connection = instance.getConnection();
                    Partition clientPartition = new ClientPartition(connection, htable.getName(), htable, clock, driver.getPartitionInfoCache());
                    Scan hscan = ((HScan)scan).unwrapDelegate();
                    SplitRegionScanner srs = new SplitRegionScanner(hscan,
                            htable,
                            connection,
                            clock,
                            clientPartition);
                    final HRegion hregion = srs.getRegion();
                    ExecRow template = SMSQLUtil.getExecRow(getExecRowTypeFormatIds());
                    spliceOperation.registerCloseable(new AutoCloseable() {
                        @Override
                        public void close() throws Exception {
                            hregion.close();
                        }
                    });


                    long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
                    TransactionalRegion region=SIDriver.driver().transactionalPartition(conglomId,new RegionPartition(hregion));
                    this.region(region)
                            .template(template)
                            .scan(scan)
                            .scanner(new RegionDataScanner(new RegionPartition(hregion),srs,Metrics.noOpMetricFactory()));

                    TableScannerIterator tableScannerIterator = new TableScannerIterator(this, spliceOperation);
                    spliceOperation.registerCloseable(tableScannerIterator);
                    return new ControlDataSet(tableScannerIterator);
                }catch(IOException e){
                    throw Exceptions.parseException(e);
                }
            }
        };
    }

    @Override
    public <Op extends SpliceOperation,V> IndexScanSetBuilder<V> newIndexScanSet(final Op spliceOperation, final String tableName) throws StandardException{
        return new IndexTableScannerBuilder<V>() {
            @Override
            public DataSet<V> buildDataSet() throws StandardException{
                rowDecodingMap(indexColToMainColPosMap);
                try{
                    TableName tableInfo = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration()).getTableInfo(tableName);
                    HTable htable = new HTable(HBaseConfiguration.create(HConfiguration.unwrapDelegate()), tableInfo);

                    SIDriver driver = SIDriver.driver();

                    HBaseConnectionFactory instance = HBaseConnectionFactory.getInstance(driver.getConfiguration());
                    Clock clock = driver.getClock();
                    Connection connection = instance.getConnection();
                    Partition clientPartition = new ClientPartition(connection, htable.getName(), htable, clock, driver.getPartitionInfoCache());
                    Scan hscan = ((HScan)scan).unwrapDelegate();
                    SplitRegionScanner srs = new SplitRegionScanner(hscan,
                            htable,
                            connection,
                            clock,
                            clientPartition);
                    final HRegion hregion = srs.getRegion();
                    ExecRow template = SMSQLUtil.getExecRow(getExecRowTypeFormatIds());
                    spliceOperation.registerCloseable(new AutoCloseable() {
                        @Override
                        public void close() throws Exception {
                            hregion.close();
                        }
                    });


                    long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
                    TransactionalRegion region=SIDriver.driver().transactionalPartition(conglomId,new RegionPartition(hregion));
                    this.region(region)
                            .template(template)
                            .scan(scan)
                            .scanner(new RegionDataScanner(new RegionPartition(hregion),srs,Metrics.noOpMetricFactory()));

                    TableScannerIterator tableScannerIterator = new TableScannerIterator(this, spliceOperation);
                    spliceOperation.registerCloseable(tableScannerIterator);
                    return new ControlDataSet(tableScannerIterator);
                }catch(IOException e){
                    throw Exceptions.parseException(e);
                }
            }
        };
    }

}