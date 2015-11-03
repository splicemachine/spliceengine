package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.zip.GZIPInputStream;

/**
 * Created by jleach on 4/13/15.
 */
public class HregionDataSetProcessor extends ControlDataSetProcessor {
    private static final Logger LOG = Logger.getLogger(HregionDataSetProcessor.class);
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        Scan scan = siTableBuilder.getScan();
        HTableInterface htable = SpliceAccessManager.getHTable(tableName);

        try {
            SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(scan, htable);
            final HRegion hregion = splitRegionScanner.getRegion();
            SimpleMeasuredRegionScanner mrs = new SimpleMeasuredRegionScanner(splitRegionScanner, siTableBuilder.getMetricFactory());
            ExecRow template = SMSQLUtil.getExecRow(siTableBuilder.getExecRowTypeFormatIds());
            siTableBuilder.tableVersion("2.0").region(TransactionalRegions.get(hregion)).template(template).scanner(mrs).scan(scan).metricFactory(Metrics.noOpMetricFactory());
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