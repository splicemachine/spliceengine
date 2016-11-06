/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import org.apache.hadoop.hbase.client.Scan;
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
                    SplitRegionScanner srs = new SplitRegionScanner(hscan,
                            ((ClientPartition)partition).unwrapDelegate(),
                            clock,
                            partition, driver.getConfiguration());
                    final HRegion hregion = srs.getRegion();
                    ExecRow template = getTemplate();
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