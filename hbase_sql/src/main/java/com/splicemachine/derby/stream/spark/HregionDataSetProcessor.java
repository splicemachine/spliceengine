/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.storage.SplitRegionScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 *
 *
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
                    SIDriver driver = SIDriver.driver();
                    PartitionFactory tableFactory = driver.getTableFactory();
                    Partition partition = tableFactory.getTable(tableName);

                    Clock clock = driver.getClock();
                    Scan hscan = ((HScan)scan).unwrapDelegate();

                    Table table = ((SkeletonHBaseClientPartition) partition).unwrapDelegate();
                    SplitRegionScanner srs = new SplitRegionScanner(hscan,
                            table,
                            clock,
                            partition, driver.getConfiguration(), table.getConfiguration());
                    final HRegion hregion = srs.getRegion();
                    ExecRow template = getTemplate();
                    spliceOperation.registerCloseable(new AutoCloseable() {
                        @Override
                        public void close() throws Exception {
                            hregion.close();
                        }
                    });

                    String tableName = hregion.getTableDescriptor().getTableName().getQualifierAsString();
                    long conglomId = Long.parseLong(tableName);
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
