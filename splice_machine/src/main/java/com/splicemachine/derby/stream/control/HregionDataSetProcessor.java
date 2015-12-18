package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.impl.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Created by jleach on 4/13/15.
 */
public class HregionDataSetProcessor extends ControlDataSetProcessor {
    private static final Logger LOG = Logger.getLogger(HregionDataSetProcessor.class);
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String conglomerateId) throws StandardException {
        Scan scan = siTableBuilder.getScan();
        Table htable = SpliceAccessManager.getHTable(conglomerateId);

        try {
            SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(scan, htable);
            final HRegion hregion = splitRegionScanner.getRegion();
            SimpleMeasuredRegionScanner mrs = new SimpleMeasuredRegionScanner(splitRegionScanner, Metrics.noOpMetricFactory());
            ExecRow template = SMSQLUtil.getExecRow(siTableBuilder.getExecRowTypeFormatIds());
            siTableBuilder.tableVersion("2.0").region(TransactionalRegions.get(hregion)).template(template).scanner(mrs).scan(scan);
            spliceOperation.registerCloseable(new AutoCloseable() {
                @Override
                public void close() throws Exception {
                    hregion.close();
                }
            });
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        TableScannerIterator tableScannerIterator = new TableScannerIterator(siTableBuilder, spliceOperation);
        spliceOperation.registerCloseable(tableScannerIterator);
        return new ControlDataSet(tableScannerIterator);
    }
}