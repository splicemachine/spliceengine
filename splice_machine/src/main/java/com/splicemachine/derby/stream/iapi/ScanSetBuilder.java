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
 * @author Scott Fines
 *         Date: 1/7/16
 */
public interface ScanSetBuilder<V>{
    ScanSetBuilder<V> metricFactory(MetricFactory metricFactory);

    ScanSetBuilder<V> scanner(DataScanner scanner);

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

    DataSet<V> buildDataSet() throws StandardException;

    DataSet<V> buildDataSet(Object caller) throws StandardException;

    ScanSetBuilder<V> activation(Activation activation);

    String base64Encode() throws IOException, StandardException;

    DataScan getScan();

    TxnView getTxn();

    OperationContext getOperationContext();

    int[] getBaseColumnMap();

    int[] getColumnPositionMap();

    long getBaseTableConglomId();

    ExecRow getTemplate();

    ScanSetBuilder<V> pin(boolean pin);

    ScanSetBuilder<V> delimited(String delimited);

    ScanSetBuilder<V> escaped(String escaped);

    ScanSetBuilder<V> lines(String lines);

    ScanSetBuilder<V> storedAs(String storedAs);

    ScanSetBuilder<V> location(String location);

    boolean getPin();

    String getDelimited();

    String getEscaped();

    String getLines();

    String getStoredAs();

    String getLocation();

}
