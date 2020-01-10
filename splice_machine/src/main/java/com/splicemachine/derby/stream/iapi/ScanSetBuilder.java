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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.DataScanner;
import java.io.IOException;

/**
 *
 * Builder for metadata around generating a scan.
 *
 * @author Scott Fines
 *         Date: 1/7/16
 */
public interface ScanSetBuilder<V>{
    /**
     *
     * Metric Factory to use when counting information.
     *
     * @param metricFactory
     * @return
     */
    ScanSetBuilder<V> metricFactory(MetricFactory metricFactory);

    /**
     *
     * Active Scanner
     *
     * @param scanner
     * @return
     */
    ScanSetBuilder<V> scanner(DataScanner scanner);

    /**
     *
     * Template for the data.
     *
     * @param template
     * @return
     */
    ScanSetBuilder<V> template(ExecRow template);

    ScanSetBuilder<V> operationContext(OperationContext operationContext);

    ScanSetBuilder<V> scan(DataScan scan);

    ScanSetBuilder<V> transaction(TxnView txn);

    ScanSetBuilder<V> optionalProbeValue(DataValueDescriptor optionalProbeValue);

    ScanSetBuilder<V> rowDecodingMap(int[] rowDecodingMap);

    ScanSetBuilder<V> baseColumnMap(int[] baseColumnMap);


    ScanSetBuilder<V> reuseRowLocation(boolean reuseRowLocation);

    ScanSetBuilder<V> keyColumnEncodingOrder(int[] keyColumnEncodingOrder);

    ScanSetBuilder<V> keyColumnSortOrder(boolean[] keyColumnSortOrder);

    ScanSetBuilder<V> partitionByColumns(int[] partitionByColumns);

    ScanSetBuilder<V> keyColumnTypes(int[] keyColumnTypes);

    ScanSetBuilder<V> keyDecodingMap(int[] keyDecodingMap);

    ScanSetBuilder<V> accessedKeyColumns(FormatableBitSet accessedKeyColumns);

    ScanSetBuilder<V> indexName(String indexName);

    ScanSetBuilder<V> tableDisplayName(String tableDisplayName);

    ScanSetBuilder<V> tableVersion(String tableVersion);

    ScanSetBuilder<V> region(TransactionalRegion region);

    ScanSetBuilder<V> fieldLengths(int[] fieldLengths);

    ScanSetBuilder<V> columnPositionMap(int[] columnPositionMap);

    ScanSetBuilder<V> baseTableConglomId(long baseTableConglomId);

    ScanSetBuilder<V> demarcationPoint(long demarcationPoint);

    ScanSetBuilder<V> oneSplitPerRegion(boolean oneSplitPerRegion);

    ScanSetBuilder<V> useSample(boolean useSample);

    ScanSetBuilder<V> sampleFraction(double sampleFraction);

    ScanSetBuilder<V> ignoreRecentTransactions(boolean ignoreRecentTransactions);

    DataSet<V> buildDataSet() throws StandardException;

    DataSet<V> buildDataSet(Object caller) throws StandardException;

    ScanSetBuilder<V> activation(Activation activation);

    String base64Encode() throws IOException, StandardException;

    DataScan getScan();

    TxnView getTxn();

    OperationContext getOperationContext();

    int[] getBaseColumnMap();

    int[] getColumnPositionMap();

    int[] getPartitionByColumnMap();


    long getBaseTableConglomId();

    ExecRow getTemplate();

    /**
     *
     * Whether the scan should go against an in-memory version (pin)
     *
     * @param pin
     * @return
     */
    ScanSetBuilder<V> pin(boolean pin);

    /**
     *
     * Column delimitter
     *
     * @param delimited
     * @return
     */
    ScanSetBuilder<V> delimited(String delimited);

    /**
     *
     * Escaped clause for delimitters
     *
     * @param escaped
     * @return
     */
    ScanSetBuilder<V> escaped(String escaped);

    /**
     *
     * Line delimitter
     *
     * @param lines
     * @return
     */
    ScanSetBuilder<V> lines(String lines);

    /**
     *
     * Stored as type (PARQUET, AVRO, ORC, TEXTFILE)
     *
     * @param storedAs
     * @return
     */
    ScanSetBuilder<V> storedAs(String storedAs);

    /**
     *  Type of compression you  to store external files
     * @param compression
     * @return
     */

    ScanSetBuilder<V> compression(String compression);

    /**
     *
     * The Hadoop Comliant file system location.
     *
     * @param location
     * @return
     */
    ScanSetBuilder<V> location(String location);

    /**
     *
     * Retrieve whether you should go against the in-memory (pin) version of the data.
     *
     * @return
     */
    boolean getPin();

    /**
     *
     * Return how the columns are delimitted.
     *
     * @return
     */
    String getDelimited();

    /**
     *
     * Get the escape character for textfile parsing.
     *
     * @return
     */
    String getEscaped();

    /**
     *
     * Retrieve the line delimitter
     *
     * @return
     */
    String getLines();

    /**
     *
     * Retrieve the stored as type (Parquet, AVRO, ORC, Textfile)
     *
     * @return
     */
    String getStoredAs();

    /**
     *
     * Retrieve the Hadoop compliant file system.
     *
     * @return
     */
    String getLocation();

    /**
     * Whether to retrieve a sample
     * @return
     */
    boolean getUseSample();

    /**
     * If we retrieve a sample (useSample=true), what is the fraction of data to retrieve
     * @return
     */
    double getSampleFraction();

    /**
     * Whether to ignore recent transactions with a txnId greater than our begin timestamp
     * @return
     */
    boolean getIgnoreRecentTransactions();

    /**
     * Get the default row
     */
    ExecRow getDefaultRow();

    /**
     * Get the default value map
     * defaultRow and defaultValueMap are used together
     */
    FormatableBitSet getDefaultValueMap();

    /**
     * set the defaultRow
     * @return
     */
    ScanSetBuilder<V> defaultRow(ExecRow defaultRow, FormatableBitSet defaultValueMap);
}
